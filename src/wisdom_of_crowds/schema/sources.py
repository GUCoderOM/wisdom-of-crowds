"""Source dimension table — the authoritative ID + name catalog.

Every row in silver and gold carries an integer ``source_id`` that
foreign-keys into this table. IDs are **statically pre-assigned** in
:data:`SOURCE_CATALOG` rather than auto-incremented, because parallel
ingest agents cannot coordinate an auto-increment safely across
independent Spark writes.

Rules for assigning a new ID:

* Pick the next unused integer in :data:`SOURCE_CATALOG` and never reuse.
* The slug (``source`` column) is the human/URL-friendly identifier
  (``manifold``, ``ecb_spf``); the int ``source_id`` is the FK.
* The dimension table row is upserted by :func:`ensure_source_row` on
  every ingest run — first run inserts, subsequent runs bump
  ``last_edited_ts``.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_log = logging.getLogger(__name__)


class SourceColumns:
    """Column-name constants for ``wisdom_of_crowds.core.sources``."""

    SOURCE_ID       = "source_id"
    SOURCE_SLUG     = "source"
    SOURCE_NAME     = "source_name"
    CREATED_TS      = "created_ts"
    LAST_EDITED_TS  = "last_edited_ts"


SOURCES_SCHEMA: StructType = StructType([
    StructField(SourceColumns.SOURCE_ID,      IntegerType(),   nullable=False),
    StructField(SourceColumns.SOURCE_SLUG,    StringType(),    nullable=False),
    StructField(SourceColumns.SOURCE_NAME,    StringType(),    nullable=False),
    StructField(SourceColumns.CREATED_TS,     TimestampType(), nullable=False),
    StructField(SourceColumns.LAST_EDITED_TS, TimestampType(), nullable=False),
])
"""Schema for the ``wisdom_of_crowds.core.sources`` dimension table."""


@dataclass(frozen=True)
class SourceMeta:
    source_id:   int
    slug:        str
    source_name: str


# ---------------------------------------------------------------------------
# The source catalog — the single source of truth for source_id assignment.
#
# NEVER REUSE AN ID.  Deleting a source is fine, but reassigning its integer
# to a different slug will silently corrupt historical silver/gold rows.
# ---------------------------------------------------------------------------

SOURCE_CATALOG: dict[str, SourceMeta] = {
    "sweets_jar": SourceMeta(1, "sweets_jar", "Sweets Jar (Galton demo)"),
    "spf":        SourceMeta(2, "spf",        "Philadelphia Fed SPF"),
    "manifold":   SourceMeta(3, "manifold",   "Manifold Markets"),
    "ecb_spf":    SourceMeta(4, "ecb_spf",    "ECB Survey of Professional Forecasters"),
    # source_id=5 was AAII, retired (no free per-respondent feed). NEVER REUSE.
    "noaa":       SourceMeta(6, "noaa",       "NOAA GEFS Ensemble Weather"),
    "polymarket": SourceMeta(7, "polymarket", "Polymarket"),
}


def get_meta(slug: str) -> SourceMeta:
    """Return the :class:`SourceMeta` for ``slug`` or raise ``KeyError`` with
    an actionable message if the source hasn't been registered yet."""
    if slug not in SOURCE_CATALOG:
        raise KeyError(
            f"Source slug {slug!r} not in SOURCE_CATALOG. Add a new "
            f"SourceMeta entry with the next unused source_id integer "
            f"(current max: {max(m.source_id for m in SOURCE_CATALOG.values())}).",
        )
    return SOURCE_CATALOG[slug]


# ---------------------------------------------------------------------------
# Dimension-table upsert
# ---------------------------------------------------------------------------


def ensure_source_row(
    spark: SparkSession,
    target: str,
    slug: str,
    cycle_ts: dt.datetime,
) -> None:
    """Insert or update the ``wisdom_of_crowds.core.sources`` row for ``slug``.

    * First observation of a slug: row inserted with ``created_ts = cycle_ts``.
    * Later observations: ``last_edited_ts`` bumped to ``cycle_ts``,
      ``created_ts`` preserved.

    ``target`` may be a Delta table name (``catalog.schema.table``) or a
    Parquet directory — the latter is for local dev where MERGE isn't
    available; there the row is simply appended (readers should dedupe by
    ``source_id``, keeping the max ``last_edited_ts``).
    """
    meta = get_meta(slug)
    _log.info(
        "ensure_source_row",
        extra={"source_id": meta.source_id, "slug": slug, "target": target},
    )

    is_table = ("/" not in target) and (target.count(".") >= 1)

    if not is_table:
        # Local dev — append-only Parquet directory (Delta MERGE isn't
        # available). Reader-side dedupe handles history.
        row = Row(
            source_id=meta.source_id,
            source=slug,
            source_name=meta.source_name,
            created_ts=cycle_ts,
            last_edited_ts=cycle_ts,
        )
        (
            spark.createDataFrame([row], SOURCES_SCHEMA)
                 .write.mode("append").parquet(target)
        )
        return

    # Databricks — Delta MERGE. Idempotent per (source_id).
    updates = spark.createDataFrame(
        [Row(
            source_id=meta.source_id,
            source=slug,
            source_name=meta.source_name,
            created_ts=cycle_ts,
            last_edited_ts=cycle_ts,
        )],
        SOURCES_SCHEMA,
    )
    updates.createOrReplaceTempView("_wisdom_of_crowds_source_updates")

    # Table may not exist on first ever run — create it if missing.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            source_id      INT     NOT NULL,
            source         STRING  NOT NULL,
            source_name    STRING  NOT NULL,
            created_ts     TIMESTAMP NOT NULL,
            last_edited_ts TIMESTAMP NOT NULL
        ) USING DELTA
    """)

    spark.sql(f"""
        MERGE INTO {target} AS t
        USING _wisdom_of_crowds_source_updates AS s
        ON t.source_id = s.source_id
        WHEN MATCHED THEN UPDATE SET
            source          = s.source,
            source_name     = s.source_name,
            last_edited_ts  = s.last_edited_ts
        WHEN NOT MATCHED THEN INSERT (source_id, source, source_name, created_ts, last_edited_ts)
        VALUES (s.source_id, s.source, s.source_name, s.created_ts, s.last_edited_ts)
    """)
