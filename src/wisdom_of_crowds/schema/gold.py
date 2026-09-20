"""Gold-layer schema — the human-facing wisdom-of-crowds table.

One row per (source, market_id, cycle_dt). This is what your final analytics
join against. It carries what the crowd said and — when a source has ground
truth — how far off it was.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class GoldColumns:
    SOURCE                 = "source"
    MARKET_ID              = "market_id"
    QUESTION               = "question"
    QUESTION_TYPE          = "question_type"
    WISDOM                 = "wisdom"
    WISDOM_OUTCOME         = "wisdom_outcome"     # only for categorical / binary
    GUESSES                = "guesses"            # count of individuals / voters
    RESOLVED_VALUE         = "resolved_value"     # ground truth if known
    SIGNED_ERROR           = "signed_error"       # wisdom - resolved_value
    SIGNED_PERCENT_ERROR   = "signed_percent_error"
    CYCLE_DT               = "cycle_dt"


GOLD_SCHEMA: StructType = StructType([
    StructField(GoldColumns.SOURCE,               StringType(), nullable=False),
    StructField(GoldColumns.MARKET_ID,            StringType(), nullable=False),
    StructField(GoldColumns.QUESTION,             StringType(), nullable=False),
    StructField(GoldColumns.QUESTION_TYPE,        StringType(), nullable=False),
    StructField(GoldColumns.WISDOM,               DoubleType(), nullable=False),
    StructField(GoldColumns.WISDOM_OUTCOME,       StringType(), nullable=True),
    StructField(GoldColumns.GUESSES,              LongType(),   nullable=False),
    StructField(GoldColumns.RESOLVED_VALUE,       DoubleType(), nullable=True),
    StructField(GoldColumns.SIGNED_ERROR,         DoubleType(), nullable=True),
    StructField(GoldColumns.SIGNED_PERCENT_ERROR, DoubleType(), nullable=True),
    StructField(GoldColumns.CYCLE_DT,             DateType(),   nullable=False),
])
