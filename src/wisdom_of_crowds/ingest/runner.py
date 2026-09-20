"""Generic ingest runner — one entrypoint for every source.

Called as a console script (``vox-ingest``) or as a Databricks task. It:

1. Reads the ``source_id`` from a CLI arg (also usable as a notebook widget).
2. Loads the source's YAML config from ``config/sources/<source_id>.yaml``
   (or a ``--config-path`` override).
3. Instantiates the ``Source`` and calls ``extract``.
4. Writes the returned DataFrame to silver — Delta table on Databricks,
   Parquet directory locally — via the same table-vs-path heuristic used
   by :mod:`wisdom_of_crowds.aggregate`.
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
from pyspark.sql.functions import col

from wisdom_of_crowds.ingest.base import SourceConfig
from wisdom_of_crowds.ingest.registry import SOURCES
from wisdom_of_crowds.schema.silver import SILVER_SCHEMA

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestJobConfig:
    source_id:     str
    silver_output: str
    config_path:   str | None = None
    cycle_ts:      dt.datetime | None = None


def run(spark: SparkSession, cfg: IngestJobConfig) -> int:
    """Run the named source and write to silver. Returns rows written."""
    if cfg.source_id not in SOURCES:
        raise KeyError(f"Unknown source_id {cfg.source_id!r}. Registered: {sorted(SOURCES)}")

    params = _load_params(cfg.source_id, cfg.config_path)
    source = SOURCES[cfg.source_id]()
    src_cfg = SourceConfig(
        source_id=cfg.source_id,
        cycle_ts=cfg.cycle_ts or dt.datetime.now(dt.timezone.utc),
        params=params,
    )

    df: DataFrame = source.extract(spark, src_cfg)
    # Pin column order so silver stays byte-stable across sources.
    df = df.select(*[col(c) for c in SILVER_SCHEMA.fieldNames()])
    _write(df, cfg.silver_output, cfg.source_id)

    n = df.count()
    _log.info("ingest_complete", extra={
        "source_id": cfg.source_id, "rows_written": n,
        "silver_output": cfg.silver_output,
    })
    return n


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _load_params(source_id: str, override_path: str | None) -> Mapping[str, Any]:
    if override_path:
        path: pathlib.Path | None = pathlib.Path(override_path)
    else:
        here = pathlib.Path(__file__).resolve()
        # "config/" is the repo-checkout layout; "_config/" is where the wheel
        # force-includes that same tree (see pyproject.toml), and is the only
        # one that exists once the job runs from the installed wheel.
        candidates = (
            parent / config_dir / "sources" / f"{source_id}.yaml"
            for parent in [here.parent, *here.parents]
            for config_dir in ("config", "_config")
        )
        path = next((c for c in candidates if c.is_file()), None)

    if path is None or not path.is_file():
        _log.warning("no_source_config", extra={"source_id": source_id})
        return {}

    with open(path) as f:
        return yaml.safe_load(f) or {}


def _is_table_name(target: str) -> bool:
    return ("/" not in target) and (target.count(".") >= 1)


def _write(df: DataFrame, target: str, source_id: str) -> None:
    if _is_table_name(target):
        # Overwrite only this source's rows; leave every other source alone.
        (
            df.write.format("delta").mode("overwrite")
              .option("replaceWhere", f"source = '{source_id}'")
              .saveAsTable(target)
        )
    else:
        # Local dev: Parquet, one directory per source (partition-style layout).
        (df.write.mode("overwrite").parquet(f"{target}/source={source_id}"))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Wisdom-of-crowds generic ingest runner.")
    p.add_argument("--source-id",     required=True, help="Source slug, e.g. 'spf' or 'sweets_jar'.")
    p.add_argument("--silver-output", required=True, help="Delta table name or Parquet directory.")
    p.add_argument("--config-path",   default=None,   help="Optional YAML config override.")
    return p


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args(argv)
    spark = SparkSession.builder.appName(f"vox-ingest-{args.source_id}").getOrCreate()
    run(spark, IngestJobConfig(source_id=args.source_id,
                               silver_output=args.silver_output,
                               config_path=args.config_path))


if __name__ == "__main__":  # pragma: no cover
    main()
