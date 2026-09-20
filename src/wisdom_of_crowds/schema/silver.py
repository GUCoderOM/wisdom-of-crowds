"""Silver-layer schema — one row per (source, question) at a given cycle.

This is the *normalised* representation every ingestion source writes into.
The aggregate job reads only this shape, dispatches on ``question_type``,
and writes to :mod:`wisdom_of_crowds.schema.gold`.

Design principle: **every source, however its raw payload is shaped, must
flatten into this schema before it leaves the ingest layer.** Different
sources exercise different columns:

* ``numeric_guesses`` questions (sweets jar, Philadelphia Fed SPF): populate
  ``guesses``; ``outcomes`` and ``prices`` are null.
* ``binary`` / ``categorical`` (Polymarket, Manifold binary/MC markets):
  populate ``outcomes`` and ``prices``; ``guesses`` is null.
* ``poll_categorical`` (Manifold POLL markets): populate ``outcomes`` and
  ``prices`` (prices = normalised vote shares).

Two schemas live here:

* :data:`INGEST_SCHEMA` — what a :class:`Source.extract` returns. Carries the
  human-readable ``source`` slug + ``cycle_ts`` but *not* the integer
  ``source_id`` or the ``cycle_dt`` partition key.
* :data:`SILVER_SCHEMA` — the enriched shape actually written to the silver
  Delta table. The runner adds ``source_id`` (from
  :data:`~wisdom_of_crowds.schema.sources.SOURCE_CATALOG`) and ``cycle_dt``
  (derived from ``cycle_ts``) before writing.

Splitting these means individual sources never need to know their integer
ID and stay trivial to unit-test.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SilverColumns:
    """Column-name constants for the silver table."""

    SOURCE_ID         = "source_id"        # INT FK into vox_populi_sources
    SOURCE            = "source"           # human/URL slug
    MARKET_ID         = "market_id"
    QUESTION          = "question"
    QUESTION_TYPE     = "question_type"
    GUESSES           = "guesses"
    OUTCOMES          = "outcomes"
    PRICES            = "prices"
    TRADER_COUNT      = "trader_count"
    VOLUME_USD        = "volume_usd"
    END_DATE          = "end_date"
    IS_RESOLVED       = "is_resolved"
    RESOLVED_OUTCOME  = "resolved_outcome"
    RESOLVED_VALUE    = "resolved_value"
    CYCLE_TS          = "cycle_ts"          # exact instant of the run
    CYCLE_DT          = "cycle_dt"          # date derived from cycle_ts, partition key


class QuestionType:
    """Closed set of question shapes the aggregator knows how to handle."""

    NUMERIC_GUESSES   = "numeric_guesses"
    BINARY            = "binary"
    CATEGORICAL       = "categorical"
    POLL_CATEGORICAL  = "poll_categorical"


# The shape a Source.extract() returns — no source_id or cycle_dt yet.
INGEST_SCHEMA: StructType = StructType([
    StructField(SilverColumns.SOURCE,           StringType(),               nullable=False),
    StructField(SilverColumns.MARKET_ID,        StringType(),               nullable=False),
    StructField(SilverColumns.QUESTION,         StringType(),               nullable=False),
    StructField(SilverColumns.QUESTION_TYPE,    StringType(),               nullable=False),
    StructField(SilverColumns.GUESSES,          ArrayType(DoubleType()),    nullable=True),
    StructField(SilverColumns.OUTCOMES,         ArrayType(StringType()),    nullable=True),
    StructField(SilverColumns.PRICES,           ArrayType(DoubleType()),    nullable=True),
    StructField(SilverColumns.TRADER_COUNT,     LongType(),                 nullable=True),
    StructField(SilverColumns.VOLUME_USD,       DoubleType(),               nullable=True),
    StructField(SilverColumns.END_DATE,         TimestampType(),            nullable=True),
    StructField(SilverColumns.IS_RESOLVED,      BooleanType(),              nullable=False),
    StructField(SilverColumns.RESOLVED_OUTCOME, StringType(),               nullable=True),
    StructField(SilverColumns.RESOLVED_VALUE,   DoubleType(),               nullable=True),
    StructField(SilverColumns.CYCLE_TS,         TimestampType(),            nullable=False),
])
"""Return shape for :meth:`wisdom_of_crowds.ingest.base.Source.extract`."""


# The enriched, on-disk silver schema. The runner adds source_id + cycle_dt
# before writing. Partition keys (source_id, cycle_dt) come LAST so that
# ``partitionBy`` doesn't shuffle the logical column order in analytics
# queries — but they're on every row.
SILVER_SCHEMA: StructType = StructType([
    StructField(SilverColumns.SOURCE_ID,        IntegerType(),              nullable=False),
    StructField(SilverColumns.SOURCE,           StringType(),               nullable=False),
    StructField(SilverColumns.MARKET_ID,        StringType(),               nullable=False),
    StructField(SilverColumns.QUESTION,         StringType(),               nullable=False),
    StructField(SilverColumns.QUESTION_TYPE,    StringType(),               nullable=False),
    StructField(SilverColumns.GUESSES,          ArrayType(DoubleType()),    nullable=True),
    StructField(SilverColumns.OUTCOMES,         ArrayType(StringType()),    nullable=True),
    StructField(SilverColumns.PRICES,           ArrayType(DoubleType()),    nullable=True),
    StructField(SilverColumns.TRADER_COUNT,     LongType(),                 nullable=True),
    StructField(SilverColumns.VOLUME_USD,       DoubleType(),               nullable=True),
    StructField(SilverColumns.END_DATE,         TimestampType(),            nullable=True),
    StructField(SilverColumns.IS_RESOLVED,      BooleanType(),              nullable=False),
    StructField(SilverColumns.RESOLVED_OUTCOME, StringType(),               nullable=True),
    StructField(SilverColumns.RESOLVED_VALUE,   DoubleType(),               nullable=True),
    StructField(SilverColumns.CYCLE_TS,         TimestampType(),            nullable=False),
    StructField(SilverColumns.CYCLE_DT,         DateType(),                 nullable=False),
])
"""Canonical silver schema: enriched with ``source_id`` (FK) and
``cycle_dt`` (partition key)."""
