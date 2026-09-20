"""Gold-layer schema — the human-facing wisdom-of-crowds table.

One row per (source_id, market_id, cycle_dt). Every row carries the
integer ``source_id`` (FK into ``wisdom_of_crowds.core.sources``) alongside the
human-readable ``source`` slug, matching the silver shape. The table is
partitioned by ``(cycle_dt, source_id)`` so parallel ingest agents can
truncate-load their own partition without touching any other.
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


class GoldColumns:
    SOURCE_ID              = "source_id"          # INT FK into wisdom_of_crowds.core.sources
    SOURCE                 = "source"             # human/URL slug
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
    StructField(GoldColumns.SOURCE_ID,            IntegerType(), nullable=False),
    StructField(GoldColumns.SOURCE,               StringType(),  nullable=False),
    StructField(GoldColumns.MARKET_ID,            StringType(),  nullable=False),
    StructField(GoldColumns.QUESTION,             StringType(),  nullable=False),
    StructField(GoldColumns.QUESTION_TYPE,        StringType(),  nullable=False),
    # NULL wisdom is legitimate: it means silver had a row for this
    # market but the strategy could not compute an answer (e.g. a
    # multi-outcome market where every price is 0). Keeping the row in
    # gold with NULL wisdom preserves the silver->gold 1:1 mapping and
    # makes "which questions couldn't we answer" a queryable signal
    # instead of a hidden diff between the two tables.
    StructField(GoldColumns.WISDOM,               DoubleType(),  nullable=True),
    StructField(GoldColumns.WISDOM_OUTCOME,       StringType(),  nullable=True),
    StructField(GoldColumns.GUESSES,              LongType(),    nullable=False),
    StructField(GoldColumns.RESOLVED_VALUE,       DoubleType(),  nullable=True),
    StructField(GoldColumns.SIGNED_ERROR,         DoubleType(),  nullable=True),
    StructField(GoldColumns.SIGNED_PERCENT_ERROR, DoubleType(),  nullable=True),
    StructField(GoldColumns.CYCLE_DT,             DateType(),    nullable=False),
])
