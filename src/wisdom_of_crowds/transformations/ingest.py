"""``ingest_<slug>`` transformations.

Each source class in :mod:`wisdom_of_crowds.ingest.sources` is wrapped
here as one named transformation. The wrapper:

1. Invokes ``Source.extract(spark, SourceConfig)`` to get the ingest-shaped
   guesses DataFrame.
2. Enriches every guesses row with ``source_id`` (looked up from the
   :data:`~wisdom_of_crowds.schema.sources.SOURCE_CATALOG`) and
   ``cycle_dt`` (derived from ``cycle_ts``) so it matches
   :data:`~wisdom_of_crowds.schema.guesses.GUESSES_SCHEMA`.
3. For prediction-market sources that also collect per-trade fidelity
   (Manifold, Polymarket), pulls the accumulated bets DataFrame off the
   source and enriches it.
4. Emits a one-row ``sources`` DataFrame to upsert the dimension table.

Every transformation returns a ``dict[str, DataFrame]``; the framework
handles the writes.
"""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import Any, Mapping, MutableMapping

import yaml
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from wisdom_of_crowds.framework import OutputSpec, Transformation, WriteMode, register
from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.ingest.sources import (
    ecb_spf,
    manifold,
    noaa,
    polymarket,
    spf,
    sweets_jar,
)
from wisdom_of_crowds.schema.bets import BETS_SCHEMA, BetColumns
from wisdom_of_crowds.schema.guesses import GUESSES_SCHEMA, GuessColumns
from wisdom_of_crowds.schema.sources import SOURCES_SCHEMA, SourceColumns, get_meta

_log = logging.getLogger(__name__)


class IngestTransformation:
    """Generic ingest-transformation wrapper. Instantiate one per source."""

    def __init__(
        self,
        *,
        name:        str,
        source_cls:  type[Source],
        has_bets:    bool = False,
    ) -> None:
        self.name        = name
        self.source_cls  = source_cls
        self.has_bets    = has_bets
        self.input_keys: tuple[str, ...] = ()

        _partition_replace_where = (
            "source_id = {source_id} AND cycle_dt = date'{cycle_dt}'"
        )
        specs: dict[str, OutputSpec] = {
            "guesses": OutputSpec(
                write_mode=WriteMode.OVERWRITE_PARTITION,
                partition_by=("cycle_dt", "source_id"),
                replace_where=_partition_replace_where,
            ),
            "sources": OutputSpec(
                write_mode=WriteMode.MERGE_ON_KEY,
                merge_keys=("source_id",),
            ),
        }
        if has_bets:
            specs["bets"] = OutputSpec(
                write_mode=WriteMode.OVERWRITE_PARTITION,
                partition_by=("cycle_dt", "source_id"),
                replace_where=_partition_replace_where,
            )
        self.output_specs: Mapping[str, OutputSpec] = specs

    def run(
        self,
        spark:  SparkSession,
        inputs: Mapping[str, DataFrame],
        params: MutableMapping[str, Any],
    ) -> Mapping[str, DataFrame]:
        slug     = self.source_cls.slug
        meta     = get_meta(slug)
        cycle_ts = _resolve_cycle_ts(params)
        cycle_dt = cycle_ts.date()

        # Inject the resolved partition values back into params so the
        # framework's write-side placeholder fill (which uses
        # ``replace_where`` templates like
        # ``source_id = {source_id} AND cycle_dt = date'{cycle_dt}'``) has
        # values to work with. Params is a mutable dict flowing through
        # :func:`run_payload`.
        params["source_id"] = meta.source_id
        params.setdefault("cycle_dt", cycle_dt.isoformat())

        # Merge the source's on-disk YAML config into what we pass into
        # SourceConfig.params. The framework payload only carries pipeline
        # metadata (cycle_dt etc.); per-source knobs (input paths,
        # limits, bets_output) live in the YAML.
        source_params: dict[str, Any] = _load_source_yaml(slug)
        source_params.update(params)  # framework params take precedence
        # Legacy in-source bets writes are handled by the framework now.
        source_params.pop("bets_output", None)

        src = self.source_cls()
        cfg = SourceConfig(slug=slug, cycle_ts=cycle_ts, params=source_params)
        guesses_ingest: DataFrame = src.extract(spark, cfg)

        # Enrich to the on-disk GUESSES_SCHEMA (adds source_id + cycle_dt).
        guesses = _enrich_with_source_and_date(
            guesses_ingest, source_id=meta.source_id, cycle_dt=cycle_dt,
            schema=GUESSES_SCHEMA,
        )

        outputs: dict[str, DataFrame] = {
            "guesses": guesses,
            "sources": _sources_row_df(spark, meta.source_id, slug, meta.source_name, cycle_ts),
        }

        if self.has_bets:
            bets_df = _extract_bets_df(src)
            outputs["bets"] = bets_df if bets_df is not None else spark.createDataFrame([], BETS_SCHEMA)

        return outputs


# ---------------------------------------------------------------------------
# Register one instance per source
# ---------------------------------------------------------------------------

register(IngestTransformation(name="ingest_sweets_jar", source_cls=sweets_jar.SweetsJarSource))
register(IngestTransformation(name="ingest_spf",        source_cls=spf.SPFSource))
register(IngestTransformation(name="ingest_ecb_spf",    source_cls=ecb_spf.ECBSPFSource))
register(IngestTransformation(name="ingest_noaa",       source_cls=noaa.NOAASource))
register(IngestTransformation(name="ingest_manifold",   source_cls=manifold.ManifoldSource,   has_bets=True))
register(IngestTransformation(name="ingest_polymarket", source_cls=polymarket.PolymarketSource, has_bets=True))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _load_source_yaml(slug: str) -> dict[str, Any]:
    """Load ``config/sources/<slug>.yaml`` from either the repo checkout
    or the wheel's packaged ``_config/`` sibling. Returns ``{}`` when
    absent — sources may declare no YAML at all."""
    here = pathlib.Path(__file__).resolve()
    candidates = (
        parent / config_dir / "sources" / f"{slug}.yaml"
        for parent in [here.parent, *here.parents]
        for config_dir in ("config", "_config")
    )
    path = next((c for c in candidates if c.is_file()), None)
    if path is None:
        _log.info("no_source_yaml", extra={"slug": slug})
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _resolve_cycle_ts(params: Mapping[str, Any]) -> dt.datetime:
    """Pull ``cycle_ts`` from params, or derive from ``cycle_dt``, else now()."""
    ts = params.get("cycle_ts")
    if isinstance(ts, dt.datetime):
        return ts
    if isinstance(ts, str):
        return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    d = params.get("cycle_dt")
    if isinstance(d, dt.date):
        return dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc)
    if isinstance(d, str):
        return dt.datetime.combine(
            dt.date.fromisoformat(d), dt.time.min, tzinfo=dt.timezone.utc,
        )
    return dt.datetime.now(dt.timezone.utc)


def _enrich_with_source_and_date(
    df: DataFrame,
    *,
    source_id: int,
    cycle_dt:  dt.date,
    schema,
) -> DataFrame:
    """Add ``source_id`` + ``cycle_dt`` columns and re-order into ``schema``."""
    enriched = (
        df.withColumn("source_id", F.lit(source_id).cast("int"))
          .withColumn("cycle_dt",  F.lit(cycle_dt).cast("date"))
    )
    return enriched.select(*[F.col(f.name) for f in schema.fields])


def _extract_bets_df(source: Source) -> DataFrame | None:
    """Sources that emit per-trade fidelity stash it here as
    ``source._bets_df`` (a DataFrame already in BETS_SCHEMA). Manifold
    and Polymarket populate this inside their :meth:`extract`.
    """
    return getattr(source, "_bets_df", None)


def _sources_row_df(
    spark:       SparkSession,
    source_id:   int,
    slug:        str,
    source_name: str,
    cycle_ts:    dt.datetime,
) -> DataFrame:
    """Build a one-row DataFrame for the ``sources`` dimension upsert."""
    row = Row(
        source_id=source_id,
        source=slug,
        source_name=source_name,
        created_ts=cycle_ts,
        last_edited_ts=cycle_ts,
    )
    return spark.createDataFrame([row], SOURCES_SCHEMA)
