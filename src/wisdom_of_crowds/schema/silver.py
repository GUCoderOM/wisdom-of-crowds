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
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SilverColumns:
    """Column-name constants for the silver table."""

    SOURCE            = "source"
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
    CYCLE_TS          = "cycle_ts"


class QuestionType:
    """Closed set of question shapes the aggregator knows how to handle."""

    NUMERIC_GUESSES   = "numeric_guesses"
    BINARY            = "binary"
    CATEGORICAL       = "categorical"
    POLL_CATEGORICAL  = "poll_categorical"


SILVER_SCHEMA: StructType = StructType([
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
"""The canonical silver schema. Every ingest source must produce rows
conforming to this shape."""
