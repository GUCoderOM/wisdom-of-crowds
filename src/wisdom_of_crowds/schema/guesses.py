"""``guesses`` — one row per (source, market, day).

Every ingestion source flattens its raw payload into this shape. Each row
bundles up the crowd's individual answers for a single question at a
single point in time:

* ``numeric_guesses`` questions (sweets jar, SPF, NOAA ensembles) populate
  the ``guesses`` array; ``outcomes`` and ``prices`` are null.
* ``binary`` / ``categorical`` markets (Manifold, Polymarket) populate
  ``outcomes`` and ``prices``; ``guesses`` is null. Per-trade fidelity for
  these sources lives in :mod:`wisdom_of_crowds.schema.bets`.
* ``poll_categorical`` (Manifold polls) also uses ``outcomes`` and
  ``prices`` (prices = normalised vote shares).

Two schemas live here:

* :data:`INGEST_SCHEMA` — what a :class:`Source.extract` returns. Carries
  the human-readable ``source`` slug + ``cycle_ts`` but *not* the integer
  ``source_id`` or the ``cycle_dt`` partition key.
* :data:`GUESSES_SCHEMA` — the enriched shape actually written to the
  ``guesses`` Delta table. The runner adds ``source_id`` (from
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


class GuessColumns:
    """Column-name constants for the ``guesses`` table."""

    SOURCE_ID         = "source_id"        # INT FK into wisdom_of_crowds.core.sources
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


# What a Source.extract() returns — no source_id or cycle_dt yet.
INGEST_SCHEMA: StructType = StructType([
    StructField(GuessColumns.SOURCE,           StringType(),               nullable=False),
    StructField(GuessColumns.MARKET_ID,        StringType(),               nullable=False),
    StructField(GuessColumns.QUESTION,         StringType(),               nullable=False),
    StructField(GuessColumns.QUESTION_TYPE,    StringType(),               nullable=False),
    StructField(GuessColumns.GUESSES,          ArrayType(DoubleType()),    nullable=True),
    StructField(GuessColumns.OUTCOMES,         ArrayType(StringType()),    nullable=True),
    StructField(GuessColumns.PRICES,           ArrayType(DoubleType()),    nullable=True),
    StructField(GuessColumns.TRADER_COUNT,     LongType(),                 nullable=True),
    StructField(GuessColumns.VOLUME_USD,       DoubleType(),               nullable=True),
    StructField(GuessColumns.END_DATE,         TimestampType(),            nullable=True),
    StructField(GuessColumns.IS_RESOLVED,      BooleanType(),              nullable=False),
    StructField(GuessColumns.RESOLVED_OUTCOME, StringType(),               nullable=True),
    StructField(GuessColumns.RESOLVED_VALUE,   DoubleType(),               nullable=True),
    StructField(GuessColumns.CYCLE_TS,         TimestampType(),            nullable=False),
])
"""Return shape for :meth:`wisdom_of_crowds.ingest.base.Source.extract`."""


# The enriched, on-disk ``guesses`` schema. The runner adds source_id and
# cycle_dt before writing. Partition keys (cycle_dt, source_id) are in the
# schema but ``partitionBy`` re-arranges them physically, so the logical
# column order is unaffected for readers.
GUESSES_SCHEMA: StructType = StructType([
    StructField(GuessColumns.SOURCE_ID,        IntegerType(),              nullable=False),
    StructField(GuessColumns.SOURCE,           StringType(),               nullable=False),
    StructField(GuessColumns.MARKET_ID,        StringType(),               nullable=False),
    StructField(GuessColumns.QUESTION,         StringType(),               nullable=False),
    StructField(GuessColumns.QUESTION_TYPE,    StringType(),               nullable=False),
    StructField(GuessColumns.GUESSES,          ArrayType(DoubleType()),    nullable=True),
    StructField(GuessColumns.OUTCOMES,         ArrayType(StringType()),    nullable=True),
    StructField(GuessColumns.PRICES,           ArrayType(DoubleType()),    nullable=True),
    StructField(GuessColumns.TRADER_COUNT,     LongType(),                 nullable=True),
    StructField(GuessColumns.VOLUME_USD,       DoubleType(),               nullable=True),
    StructField(GuessColumns.END_DATE,         TimestampType(),            nullable=True),
    StructField(GuessColumns.IS_RESOLVED,      BooleanType(),              nullable=False),
    StructField(GuessColumns.RESOLVED_OUTCOME, StringType(),               nullable=True),
    StructField(GuessColumns.RESOLVED_VALUE,   DoubleType(),               nullable=True),
    StructField(GuessColumns.CYCLE_TS,         TimestampType(),            nullable=False),
    StructField(GuessColumns.CYCLE_DT,         DateType(),                 nullable=False),
])
"""Canonical ``guesses`` schema: enriched with ``source_id`` (FK) and
``cycle_dt`` (partition key)."""


# ---------------------------------------------------------------------------
# Backwards-compat shims. Prefer the new names above; these aliases exist
# so nothing imports break during the rename.
# ---------------------------------------------------------------------------

SilverColumns = GuessColumns
SILVER_SCHEMA = GUESSES_SCHEMA
