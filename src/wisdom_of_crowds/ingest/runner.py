"""Generic ingest runner — one entrypoint for every source.

Called as a console script (``vox-ingest``) or as a Databricks task. It:

1. Reads the ``source_slug`` from a CLI arg (also usable as a notebook widget).
2. Loads the source's YAML config from ``config/sources/<slug>.yaml``
   (or a ``--config-path`` override).
3. Instantiates the ``Source`` and calls ``extract`` — the source returns a
   DataFrame in :data:`~wisdom_of_crowds.schema.silver.INGEST_SCHEMA` shape.
4. **Enriches** each row with the integer ``source_id`` (looked up from
   :data:`~wisdom_of_crowds.schema.sources.SOURCE_CATALOG`) and ``cycle_dt``
   (derived from ``cycle_ts``) so it matches
   :data:`~wisdom_of_crowds.schema.silver.SILVER_SCHEMA`.
5. Upserts the dimension row in ``wisdom_of_crowds.core.sources``.
6. Writes silver, partitioned by ``(cycle_dt, source_id)``, with a
   ``replaceWhere`` that scopes the truncate-load to this run's partition
   only — so running the same source twice in one day is idempotent and
   different sources never touch each other's data.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
from dataclasses import dataclass
from typing import Any, Mapping

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wisdom_of_crowds.ingest.base import SourceConfig
from wisdom_of_crowds.ingest.registry import SOURCES
from wisdom_of_crowds.schema.silver import SILVER_SCHEMA, SilverColumns
from wisdom_of_crowds.schema.sources import ensure_source_row, get_meta

_log = logging.getLogger(__name__)

# The dimension table lives next to silver. Callers may override via
# IngestJobConfig.sources_output; if unset the runner derives it from
# silver_output by swapping the last name segment.
_DEFAULT_SOURCES_TABLE = "wisdom_of_crowds.core.sources"


@dataclass(frozen=True)
class IngestJobConfig:
    source_slug:    str
    silver_output:  str
    config_path:    str | None = None
    cycle_ts:       dt.datetime | None = None
    sources_output: str | None = None  # dimension table target; default derived from silver_output


def run(spark: SparkSession, cfg: IngestJobConfig) -> int:
    """Run the named source and write to silver. Returns rows written."""
    if cfg.source_slug not in SOURCES:
        raise KeyError(
            f"Unknown source slug {cfg.source_slug!r}. Registered: {sorted(SOURCES)}",
        )

    params    = _load_params(cfg.source_slug, cfg.config_path)
    source    = SOURCES[cfg.source_slug]()
    cycle_ts  = cfg.cycle_ts or dt.datetime.now(dt.timezone.utc)
    src_cfg   = SourceConfig(slug=cfg.source_slug, cycle_ts=cycle_ts, params=params)
    meta      = get_meta(cfg.source_slug)
    cycle_dt  = cycle_ts.date()

    # Upsert dimension row FIRST so the FK integrity holds even if a reader
    # peeks at silver before the writer commits.
    sources_target = cfg.sources_output or _derive_sources_target(cfg.silver_output)
    ensure_source_row(spark, sources_target, cfg.source_slug, cycle_ts)

    # Source produces INGEST_SCHEMA rows; runner enriches to SILVER_SCHEMA.
    ingested: DataFrame = source.extract(spark, src_cfg)
    silver: DataFrame = _enrich(ingested, source_id=meta.source_id, cycle_dt=cycle_dt)

    _write(silver, cfg.silver_output, source_id=meta.source_id, cycle_dt=cycle_dt)

    n = silver.count()
    _log.info("ingest_complete", extra={
        "source_slug":    cfg.source_slug,
        "source_id":      meta.source_id,
        "rows_written":   n,
        "silver_output":  cfg.silver_output,
        "sources_output": sources_target,
        "cycle_dt":       cycle_dt.isoformat(),
    })
    return n


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _enrich(df: DataFrame, *, source_id: int, cycle_dt: dt.date) -> DataFrame:
    """Add ``source_id`` and ``cycle_dt`` to a source's INGEST_SCHEMA output
    and reorder into SILVER_SCHEMA."""
    enriched = (
        df.withColumn(SilverColumns.SOURCE_ID, F.lit(source_id).cast("int"))
          .withColumn(SilverColumns.CYCLE_DT,  F.lit(cycle_dt).cast("date"))
    )
    return enriched.select(*[F.col(c) for c in SILVER_SCHEMA.fieldNames()])


def _derive_sources_target(silver_output: str) -> str:
    """Given a silver table name or path, return the sources dimension
    table sibling. Table naming convention: same catalog + schema as
    silver, table name ``sources``. Path convention: sibling directory
    ``sources/`` next to the silver Parquet directory.
    """
    if _is_table_name(silver_output):
        parts = silver_output.split(".")
        parts[-1] = "sources"
        return ".".join(parts)
    return str(pathlib.Path(silver_output).parent / "sources")


def _load_params(source_slug: str, override_path: str | None) -> Mapping[str, Any]:
    if override_path:
        path: pathlib.Path | None = pathlib.Path(override_path)
    else:
        here = pathlib.Path(__file__).resolve()
        # "config/" is the repo-checkout layout; "_config/" is where the wheel
        # force-includes that same tree (see pyproject.toml).
        candidates = (
            parent / config_dir / "sources" / f"{source_slug}.yaml"
            for parent in [here.parent, *here.parents]
            for config_dir in ("config", "_config")
        )
        path = next((c for c in candidates if c.is_file()), None)

    if path is None or not path.is_file():
        _log.warning("no_source_config", extra={"source_slug": source_slug})
        return {}

    with open(path) as f:
        return yaml.safe_load(f) or {}


def _is_table_name(target: str) -> bool:
    return ("/" not in target) and (target.count(".") >= 1)


def _write(
    df: DataFrame,
    target: str,
    *,
    source_id: int,
    cycle_dt: dt.date,
) -> None:
    """Write silver, scoped to the current (source_id, cycle_dt) partition.

    Truncate-load semantics: running the same source on the same day
    overwrites just that partition. A different source, or the same source
    on a different day, is left alone. This makes parallel writes from
    multiple ingest agents safe.
    """
    replace_where = (
        f"source_id = {source_id} AND cycle_dt = date'{cycle_dt.isoformat()}'"
    )
    if _is_table_name(target):
        (
            df.write.format("delta").mode("overwrite")
              .option("replaceWhere", replace_where)
              .partitionBy(SilverColumns.CYCLE_DT, SilverColumns.SOURCE_ID)
              .saveAsTable(target)
        )
    else:
        # Local dev: Parquet, partition-style layout per (cycle_dt, source_id).
        (
            df.write.mode("overwrite")
              .partitionBy(SilverColumns.CYCLE_DT, SilverColumns.SOURCE_ID)
              .parquet(target)
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Wisdom-of-crowds generic ingest runner.")
    # `--source-id` is retained as the flag name for CLI backwards-compat,
    # but its value is the slug (not the integer PK).
    p.add_argument("--source-id",     required=True, help="Source slug (e.g. 'spf'). CLI legacy name.")
    p.add_argument("--source-slug",   default=None,  help="Alias for --source-id (preferred).")
    p.add_argument("--silver-output", required=True, help="Delta table name or Parquet directory.")
    p.add_argument("--sources-output", default=None, help="Dimension table name; default derived.")
    p.add_argument("--config-path",   default=None,  help="Optional YAML config override.")
    return p


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args(argv)
    slug = args.source_slug or args.source_id
    spark = SparkSession.builder.appName(f"vox-ingest-{slug}").getOrCreate()
    run(spark, IngestJobConfig(
        source_slug=slug,
        silver_output=args.silver_output,
        config_path=args.config_path,
        sources_output=args.sources_output,
    ))


if __name__ == "__main__":  # pragma: no cover
    main()
