"""``answers`` — the single per-guess table.

Every ingestion source, whether it publishes named forecasters or
anonymous trades, writes one row per individual data point into this
table. That means:

* Sweets jar: one row per person's guess.
* Philadelphia Fed / ECB SPF: one row per forecaster per (variable,
  target quarter, horizon).
* Manifold: one row per bet — ``name`` is the trader's userId.
* Polymarket: one row per trade — ``name`` is the trader's proxy wallet
  (or ``NULL`` when the trade was submitted anonymously).
* NOAA GEFS: one row per ensemble member per (location, variable,
  forecast hour) — ``name`` is the member number.

``wisdom`` is computed by ``GROUP BY (source_id, market_id, cycle_dt)``
over this table.

The same shape works for numeric guesses (fills ``answer_value``),
categorical outcomes (fills ``answer_outcome``), and prediction-market
trades (fills both — the outcome the trader bought and the fill price
that reveals their probability).

Two schemas live here:

* :data:`INGEST_ANSWER_SCHEMA` — what a :meth:`Source.extract` returns.
  Human ``source`` slug + ``cycle_ts``, no integer ``source_id`` or
  ``cycle_dt``.
* :data:`ANSWERS_SCHEMA` — the enriched on-disk shape. The framework
  adds ``source_id`` and ``cycle_dt`` before writing.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class AnswerColumns:
    """Column-name constants for the ``answers`` table."""

    SOURCE_ID       = "source_id"        # INT FK into wisdom_of_crowds.core.sources
    SOURCE          = "source"           # human/URL slug
    MARKET_ID       = "market_id"        # question identifier (source-scoped)
    QUESTION        = "question"         # human question text
    QUESTION_TYPE   = "question_type"    # numeric_guesses | binary | categorical | poll_categorical
    NAME            = "name"             # guesser identity (nullable — anonymous ok)
    ANSWER_VALUE    = "answer_value"     # numeric guess or market fill price (nullable)
    ANSWER_OUTCOME  = "answer_outcome"   # categorical / binary choice (nullable)
    WEIGHT          = "weight"           # stake / bet size / ensemble weight (nullable)
    CREATED_AT      = "created_at"       # when the guess was made (nullable)
    CYCLE_TS        = "cycle_ts"         # exact instant of the ingest run
    CYCLE_DT        = "cycle_dt"         # date derived from cycle_ts, partition key


class QuestionType:
    """Closed set of question shapes the aggregator knows how to handle."""

    NUMERIC_GUESSES  = "numeric_guesses"
    BINARY           = "binary"
    CATEGORICAL      = "categorical"
    POLL_CATEGORICAL = "poll_categorical"


# What a Source.extract() returns — no source_id or cycle_dt yet.
INGEST_ANSWER_SCHEMA: StructType = StructType([
    StructField(AnswerColumns.SOURCE,         StringType(),    nullable=False),
    StructField(AnswerColumns.MARKET_ID,      StringType(),    nullable=False),
    StructField(AnswerColumns.QUESTION,       StringType(),    nullable=False),
    StructField(AnswerColumns.QUESTION_TYPE,  StringType(),    nullable=False),
    StructField(AnswerColumns.NAME,           StringType(),    nullable=True),
    StructField(AnswerColumns.ANSWER_VALUE,   DoubleType(),    nullable=True),
    StructField(AnswerColumns.ANSWER_OUTCOME, StringType(),    nullable=True),
    StructField(AnswerColumns.WEIGHT,         DoubleType(),    nullable=True),
    StructField(AnswerColumns.CREATED_AT,     TimestampType(), nullable=True),
    StructField(AnswerColumns.CYCLE_TS,       TimestampType(), nullable=False),
])
"""Return shape for :meth:`wisdom_of_crowds.ingest.base.Source.extract`."""


# The enriched, on-disk ``answers`` schema.
ANSWERS_SCHEMA: StructType = StructType([
    StructField(AnswerColumns.SOURCE_ID,      IntegerType(),   nullable=False),
    StructField(AnswerColumns.SOURCE,         StringType(),    nullable=False),
    StructField(AnswerColumns.MARKET_ID,      StringType(),    nullable=False),
    StructField(AnswerColumns.QUESTION,       StringType(),    nullable=False),
    StructField(AnswerColumns.QUESTION_TYPE,  StringType(),    nullable=False),
    StructField(AnswerColumns.NAME,           StringType(),    nullable=True),
    StructField(AnswerColumns.ANSWER_VALUE,   DoubleType(),    nullable=True),
    StructField(AnswerColumns.ANSWER_OUTCOME, StringType(),    nullable=True),
    StructField(AnswerColumns.WEIGHT,         DoubleType(),    nullable=True),
    StructField(AnswerColumns.CREATED_AT,     TimestampType(), nullable=True),
    StructField(AnswerColumns.CYCLE_TS,       TimestampType(), nullable=False),
    StructField(AnswerColumns.CYCLE_DT,       DateType(),      nullable=False),
])
"""Canonical ``answers`` schema — enriched with ``source_id`` (FK) and
``cycle_dt`` (partition key)."""
