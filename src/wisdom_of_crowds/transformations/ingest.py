"""``ingest_<slug>`` transformations — every source emits ``answers``.

Every source class in :mod:`wisdom_of_crowds.ingest.sources` is wrapped
here as one named transformation. The wrapper:

1. Loads ``config/sources/<slug>.yaml`` and merges it into
   :class:`SourceConfig.params` — framework payload params take
   precedence.
2. Invokes ``Source.extract(spark, cfg)`` to get an
   :data:`~wisdom_of_crowds.schema.answers.INGEST_ANSWER_SCHEMA`-shaped
   DataFrame (one row per individual guess/bet/vote).
3. Enriches every row with ``source_id`` (looked up from
   :data:`~wisdom_of_crowds.schema.sources.SOURCE_CATALOG`) and
   ``cycle_dt`` (derived from ``cycle_ts``) so it matches
   :data:`~wisdom_of_crowds.schema.answers.ANSWERS_SCHEMA`.
4. Emits a one-row ``sources`` DataFrame that upserts the dimension
   table via ``MERGE`` (framework handles the SQL).

The transformation also mutates the mutable ``params`` dict to inject
its resolved ``source_id`` and ``cycle_dt``, so framework
``replace_where`` placeholders can be filled at write time.
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
from wisdom_of_crowds.schema.answers import ANSWERS_SCHEMA, AnswerColumns
from wisdom_of_crowds.schema.sources import SOURCES_SCHEMA, get_meta

_log = logging.getLogger(__name__)


class IngestTransformation:
    """Generic ingest-transformation wrapper. Instantiate one per source."""

    def __init__(self, *, name: str, source_cls: type[Source]) -> None:
        self.name        = name
        self.source_cls  = source_cls
        self.input_keys: tuple[str, ...] = ()

        _partition_replace_where = (
            "source_id = {source_id} AND cycle_dt = date'{cycle_dt}'"
        )
        self.output_specs: Mapping[str, OutputSpec] = {
            "answers": OutputSpec(
                write_mode=WriteMode.OVERWRITE_PARTITION,
                partition_by=("cycle_dt", "source_id"),
                replace_where=_partition_replace_where,
            ),
            "sources": OutputSpec(
                write_mode=WriteMode.MERGE_ON_KEY,
                merge_keys=("source_id",),
            ),
        }

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

        # Inject the resolved partition values into params so
        # framework replace-where placeholder fill has them.
        params["source_id"] = meta.source_id
        params.setdefault("cycle_dt", cycle_dt.isoformat())

        # Merge YAML source config into SourceConfig.params; framework
        # payload params win on collision.
        source_params: dict[str, Any] = _load_source_yaml(slug)
        source_params.update(params)

        src = self.source_cls()
        cfg = SourceConfig(slug=slug, cycle_ts=cycle_ts, params=source_params)
        ingest_answers: DataFrame = src.extract(spark, cfg)

        answers = _enrich_with_source_and_date(
            ingest_answers, source_id=meta.source_id, cycle_dt=cycle_dt,
        )

        return {
            "answers": answers,
            "sources": _sources_row_df(spark, meta.source_id, slug, meta.source_name, cycle_ts),
        }


# ---------------------------------------------------------------------------
# Register one instance per source
# ---------------------------------------------------------------------------

register(IngestTransformation(name="ingest_sweets_jar", source_cls=sweets_jar.SweetsJarSource))
register(IngestTransformation(name="ingest_spf",        source_cls=spf.SPFSource))
register(IngestTransformation(name="ingest_ecb_spf",    source_cls=ecb_spf.ECBSPFSource))
register(IngestTransformation(name="ingest_noaa",       source_cls=noaa.NOAASource))
register(IngestTransformation(name="ingest_manifold",   source_cls=manifold.ManifoldSource))
register(IngestTransformation(name="ingest_polymarket", source_cls=polymarket.PolymarketSource))


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
) -> DataFrame:
    """Add ``source_id`` + ``cycle_dt`` and reorder into ANSWERS_SCHEMA."""
    enriched = (
        df.withColumn(AnswerColumns.SOURCE_ID, F.lit(source_id).cast("int"))
          .withColumn(AnswerColumns.CYCLE_DT,  F.lit(cycle_dt).cast("date"))
    )
    return enriched.select(*[F.col(f.name) for f in ANSWERS_SCHEMA.fields])


def _sources_row_df(
    spark:       SparkSession,
    source_id:   int,
    slug:        str,
    source_name: str,
    cycle_ts:    dt.datetime,
) -> DataFrame:
    row = Row(
        source_id=source_id,
        source=slug,
        source_name=source_name,
        created_ts=cycle_ts,
        last_edited_ts=cycle_ts,
    )
    return spark.createDataFrame([row], SOURCES_SCHEMA)
