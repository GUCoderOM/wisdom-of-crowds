"""``wisdom`` — one row per (source, market, day) with the crowd's answer.

Downstream of :mod:`wisdom_of_crowds.schema.guesses`. The aggregator
reads guesses, applies the strategy for each ``question_type``, and writes
the resulting single-number answer here.

Partitioned by ``(cycle_dt, source_id)`` — same scheme as guesses — so
parallel ingest+aggregate runs of different sources never touch each
other's partitions.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class WisdomColumns:
    SOURCE_ID              = "source_id"          # INT FK into wisdom_of_crowds.core.sources
    SOURCE                 = "source"             # human/URL slug
    MARKET_ID              = "market_id"
    QUESTION               = "question"
    QUESTION_TYPE          = "question_type"
    WISDOM                 = "wisdom"             # the crowd's single-number answer
    WISDOM_OUTCOME         = "wisdom_outcome"     # only for categorical / binary
    GUESSES                = "guesses"            # count of individuals / voters
    RESOLVED_VALUE         = "resolved_value"     # ground truth if known
    SIGNED_ERROR           = "signed_error"       # wisdom - resolved_value
    SIGNED_PERCENT_ERROR   = "signed_percent_error"
    CYCLE_DT               = "cycle_dt"


WISDOM_SCHEMA: StructType = StructType([
    StructField(WisdomColumns.SOURCE_ID,            IntegerType(), nullable=False),
    StructField(WisdomColumns.SOURCE,               StringType(),  nullable=False),
    StructField(WisdomColumns.MARKET_ID,            StringType(),  nullable=False),
    StructField(WisdomColumns.QUESTION,             StringType(),  nullable=False),
    StructField(WisdomColumns.QUESTION_TYPE,        StringType(),  nullable=False),
    # NULL wisdom is legitimate: a market whose strategy couldn't
    # compute an answer (e.g. an empty answers array even after
    # hydration) stays in wisdom with NULL, preserving the 1:1 mapping
    # to guesses and making "which questions couldn't we answer" a
    # queryable signal.
    StructField(WisdomColumns.WISDOM,               DoubleType(),  nullable=True),
    StructField(WisdomColumns.WISDOM_OUTCOME,       StringType(),  nullable=True),
    StructField(WisdomColumns.GUESSES,              LongType(),    nullable=False),
    StructField(WisdomColumns.RESOLVED_VALUE,       DoubleType(),  nullable=True),
    StructField(WisdomColumns.SIGNED_ERROR,         DoubleType(),  nullable=True),
    StructField(WisdomColumns.SIGNED_PERCENT_ERROR, DoubleType(),  nullable=True),
    StructField(WisdomColumns.CYCLE_DT,             DateType(),    nullable=False),
])


# Backwards-compat shims — prefer the new names above.
GoldColumns  = WisdomColumns
GOLD_SCHEMA  = WISDOM_SCHEMA
