"""Aggregate ``answers`` (one row per individual guess) into ``wisdom``
(one row per market/question with the crowd's verdict).

Runs identically locally (Parquet) and on Databricks (Delta). All
partition filtering (by ``cycle_dt`` and optionally ``source_id``) is
applied by the framework before the DataFrame arrives here.

Aggregation per ``question_type``:

* **numeric_guesses** — wisdom = median of ``answer_value``.
* **binary / categorical / poll_categorical** — wisdom = mode-share
  (the fraction of the crowd that chose the modal outcome), and
  ``wisdom_outcome`` = the modal outcome itself. Same formula for two
  outcomes and N outcomes; when a market has no answers, both columns
  are ``NULL`` and the row still lands in ``wisdom`` (queryable
  data-quality signal).

``guesses`` on the wisdom row is always ``COUNT(*)`` of the individual
answers that produced it — anonymous rows count, ``name = NULL`` is
fine.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from wisdom_of_crowds.schema.answers import AnswerColumns, QuestionType
from wisdom_of_crowds.schema.wisdom import WisdomColumns


_GROUP_KEYS = [
    AnswerColumns.SOURCE_ID,
    AnswerColumns.SOURCE,
    AnswerColumns.MARKET_ID,
    AnswerColumns.QUESTION,
    AnswerColumns.QUESTION_TYPE,
]


def build_wisdom(answers: DataFrame, cycle_dt: dt.date) -> DataFrame:
    """Pure GROUP BY over ``answers`` -> wisdom rows.

    Assumes ``answers`` is already scoped to a single ``cycle_dt`` (the
    framework does that filter). Returns one row per market/question.
    """
    # Step 1 — per-market aggregates that don't need the modal outcome
    # (median + count + a preliminary mode).
    grouped = answers.groupBy(*_GROUP_KEYS).agg(
        F.count(F.lit(1)).alias("_count"),
        F.percentile_approx(F.col(AnswerColumns.ANSWER_VALUE), 0.5).alias("_median_value"),
        F.mode(F.col(AnswerColumns.ANSWER_OUTCOME)).alias("_mode_outcome"),
    )

    # Step 2 — for market question types, compute the mode's share of
    # the total answer count. Two passes are needed because "share of
    # the mode" depends on which outcome IS the mode, and Spark's
    # aggregation can't refer to another aggregate mid-groupBy.
    mode_share = (
        answers.alias("a")
               .join(
                   grouped.select(
                       AnswerColumns.SOURCE_ID,
                       AnswerColumns.MARKET_ID,
                       "_mode_outcome",
                   ).alias("m"),
                   on=[AnswerColumns.SOURCE_ID, AnswerColumns.MARKET_ID],
                   how="inner",
               )
               .groupBy(AnswerColumns.SOURCE_ID, AnswerColumns.MARKET_ID)
               .agg(
                   (
                       F.sum(
                           F.when(
                               F.col(f"a.{AnswerColumns.ANSWER_OUTCOME}")
                               == F.col("_mode_outcome"),
                               F.lit(1.0),
                           ).otherwise(F.lit(0.0)),
                       )
                       / F.count(F.lit(1))
                   ).alias("_mode_share"),
               )
    )

    combined = grouped.join(
        mode_share,
        on=[AnswerColumns.SOURCE_ID, AnswerColumns.MARKET_ID],
        how="left",
    )

    # Step 3 — pick wisdom + wisdom_outcome per question_type.
    is_numeric = F.col(AnswerColumns.QUESTION_TYPE) == F.lit(QuestionType.NUMERIC_GUESSES)

    return combined.select(
        F.col(AnswerColumns.SOURCE_ID).alias(WisdomColumns.SOURCE_ID),
        F.col(AnswerColumns.SOURCE).alias(WisdomColumns.SOURCE),
        F.col(AnswerColumns.MARKET_ID).alias(WisdomColumns.MARKET_ID),
        F.col(AnswerColumns.QUESTION).alias(WisdomColumns.QUESTION),
        F.col(AnswerColumns.QUESTION_TYPE).alias(WisdomColumns.QUESTION_TYPE),
        F.when(is_numeric, F.col("_median_value"))
         .otherwise(F.col("_mode_share"))
         .alias(WisdomColumns.WISDOM),
        F.when(is_numeric, F.lit(None).cast("string"))
         .otherwise(F.col("_mode_outcome"))
         .alias(WisdomColumns.WISDOM_OUTCOME),
        F.col("_count").cast("long").alias(WisdomColumns.GUESSES),
        F.lit(None).cast("double").alias(WisdomColumns.RESOLVED_VALUE),
        F.lit(None).cast("double").alias(WisdomColumns.SIGNED_ERROR),
        F.lit(None).cast("double").alias(WisdomColumns.SIGNED_PERCENT_ERROR),
        F.lit(cycle_dt).cast("date").alias(WisdomColumns.CYCLE_DT),
    )
