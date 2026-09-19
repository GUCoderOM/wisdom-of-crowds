"""The heart of the experiment.

Every function here is a pure DataFrame → DataFrame (or DataFrame → number)
transformation, faithful to the procedure Francis Galton described in
*Vox Populi* (Nature 75:450–451, 1907). Docstrings quote the paper next to
the step they implement, so the code and the source can be read together.

The functions are intentionally small and testable. The Jupyter notebook is
a narrative shell; the logic lives here.

.. note::

    Only the loading + cleaning stages have been lifted to the target
    engineering bar so far (schema contract, defect-reason vocabulary,
    single-pass counting, structured logging). The remaining stages
    (``crowd_median``, ``score_crowd``, ``summarise_distribution``,
    ``build_summary_row``) will be lifted in follow-up review passes.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from wisdom_of_crowds.columns import FlagColumns, InputColumns
from wisdom_of_crowds.defects import DefectReason
from wisdom_of_crowds.schema import assert_guesses_schema

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema — explicit, no inference
# ---------------------------------------------------------------------------

GUESSES_SCHEMA: StructType = StructType(
    [
        StructField(InputColumns.NAME,  StringType(),  nullable=False),
        StructField(InputColumns.GUESS, IntegerType(), nullable=False),
    ]
)
"""Schema for the input CSV. Kept explicit so schema drift is a loud failure
rather than a silent one."""


# ---------------------------------------------------------------------------
# Stage 4 — Load
# ---------------------------------------------------------------------------


def load_guesses(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the CSV of guesses at ``input_path``.

    Galton had *"about 800 tickets"* handed to him after a weight-judging
    competition at the West of England Fat Stock and Poultry Exhibition
    (*Vox Populi*, Nature 75:450). We take the same posture: read what we
    were given, verbatim, and only reject cards later if they fail the
    *defective-or-illegible* test.
    """
    return (
        spark.read.option("header", "true")
        .schema(GUESSES_SCHEMA)
        .csv(input_path)
    )


# ---------------------------------------------------------------------------
# Stage 5 — Flag defective rows (annotate, never drop)
# ---------------------------------------------------------------------------


def _defect_reasons_column() -> F.Column:
    """Build the ``array<string>`` column that lists why a row is defective.

    Each element is a :class:`DefectReason` value. Elements are added by
    ``F.when(condition, F.lit(reason))`` — the ``when`` returns ``null``
    when the condition is false, and :func:`pyspark.sql.functions.array_compact`
    then drops those nulls, so the array contains only reasons that apply.

    Extracted from :func:`flag_defective` so it can be unit-tested in
    isolation and reused if we ever need the raw expression somewhere else.
    """
    def when_reason(condition: F.Column, reason: DefectReason) -> F.Column:
        return F.when(condition, F.lit(reason.value))

    return F.array_compact(F.array(
        when_reason(
            F.col(InputColumns.NAME).isNull() | (F.trim(InputColumns.NAME) == ""),
            DefectReason.MISSING_NAME,
        ),
        when_reason(
            F.col(InputColumns.GUESS).isNull(),
            DefectReason.MISSING_GUESS,
        ),
        when_reason(
            F.col(InputColumns.GUESS) <= 0,
            DefectReason.NON_POSITIVE_GUESS,
        ),
    ))


def flag_defective(df: DataFrame) -> DataFrame:
    """Annotate each row with the reasons it is (or isn't) defective.

    Adds two columns:

    * ``defect_reasons`` — ``array<string>``. Empty array means the row is
      valid. Otherwise, each element is a :class:`DefectReason` value.
    * ``is_defective`` — ``boolean``. True iff ``defect_reasons`` is
      non-empty.

    Rows are never dropped here. Cleaning is a separate step so the flagged
    frame is inspectable and auditable end-to-end.

    Galton: *"After weeding thirteen cards out of the collection, as being
    defective or illegible, there remained 787 for discussion."*
    — Vox Populi, Nature 75:450 (1907).

    Args:
        df: DataFrame satisfying :data:`GUESSES_SCHEMA` (extra columns are
            allowed).

    Returns:
        The same DataFrame with ``defect_reasons`` and ``is_defective``
        columns appended.

    Raises:
        SchemaValidationError: If ``df`` is missing the required columns
            or has the wrong types.
    """
    assert_guesses_schema(df)
    return (
        df
        .withColumn(FlagColumns.DEFECT_REASONS, _defect_reasons_column())
        .withColumn(
            FlagColumns.IS_DEFECTIVE,
            F.size(F.col(FlagColumns.DEFECT_REASONS)) > 0,
        )
    )


# ---------------------------------------------------------------------------
# Stage 6 — Clean (drop defective rows, return full audit trail)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CleanResult:
    """The result of the cleaning step: cleaned frame plus a full audit trail.

    Attributes:
        kept:              Cleaned DataFrame — only :data:`InputColumns.NAME`
                           and :data:`InputColumns.GUESS`, with the flag
                           columns dropped so nothing downstream leaks a
                           defect reason into an analytical result.
        n_input:           Rows on the flagged DataFrame before cleaning.
        n_kept:            Rows on ``kept`` after cleaning. Derived as
                           ``n_input - n_dropped`` so it can never disagree.
        n_dropped:         ``n_input - n_kept``.
        dropped_by_reason: Count of dropped rows per :class:`DefectReason`.
                           A row failing two checks contributes to both
                           counters — so the sum of these counts can exceed
                           ``n_dropped`` and that is correct.
    """

    kept:              DataFrame
    n_input:           int
    n_kept:            int
    n_dropped:         int
    dropped_by_reason: Mapping[DefectReason, int]


def clean_guesses(flagged: DataFrame) -> CleanResult:
    """Drop the rows flagged as defective and return a full audit trail.

    Computes every count in a **single aggregation pass** over the flagged
    DataFrame, then produces the cleaned frame by filtering. The counts are
    emitted as a structured log record so the drop rate is visible to any
    observability layer the notebook is plugged into.

    Args:
        flagged: The output of :func:`flag_defective`.

    Returns:
        A :class:`CleanResult` carrying the cleaned frame and the counts.
    """
    per_reason_aggregations = [
        F.sum(
            F.when(
                F.array_contains(F.col(FlagColumns.DEFECT_REASONS), reason.value),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias(f"n_{reason.value}")
        for reason in DefectReason
    ]

    totals_row = flagged.agg(
        F.count(F.lit(1)).alias("n_input"),
        F.sum(
            F.when(F.col(FlagColumns.IS_DEFECTIVE), F.lit(1)).otherwise(F.lit(0))
        ).alias("n_dropped"),
        *per_reason_aggregations,
    ).collect()[0]

    kept = (
        flagged
        .where(~F.col(FlagColumns.IS_DEFECTIVE))
        .select(InputColumns.NAME, InputColumns.GUESS)
    )

    n_input   = int(totals_row["n_input"])
    n_dropped = int(totals_row["n_dropped"] or 0)
    dropped_by_reason: dict[DefectReason, int] = {
        reason: int(totals_row[f"n_{reason.value}"] or 0)
        for reason in DefectReason
    }

    _log.info(
        "cleaning_complete",
        extra={
            "n_input":           n_input,
            "n_kept":            n_input - n_dropped,
            "n_dropped":         n_dropped,
            "dropped_by_reason": {r.value: v for r, v in dropped_by_reason.items()},
        },
    )

    return CleanResult(
        kept=kept,
        n_input=n_input,
        n_kept=n_input - n_dropped,
        n_dropped=n_dropped,
        dropped_by_reason=dropped_by_reason,
    )


# ---------------------------------------------------------------------------
# Stage 7 — The crowd's statistic
# ---------------------------------------------------------------------------
# NOTE: The functions below have not yet been lifted to the same engineering
# bar as loading/cleaning above. They will be reviewed and rewritten in the
# next pass. Behaviour is identical to the previous version.
# ---------------------------------------------------------------------------


def crowd_median(guesses: DataFrame) -> int:
    """Compute the crowd's *middlemost estimate*.

    From Galton's *One Vote, One Value* (Nature 75:414):

        "That conclusion is clearly not the average of all the estimates,
        which would give a voting power to 'cranks' in proportion to their
        crankiness... I wish to point out that the estimate to which least
        objection can be raised is the middlemost estimate."

    We use ``pyspark.sql.functions.median`` — added in Spark 3.4, present in
    the Spark 4.0.0 that ships with Databricks Runtime 17.3 LTS. It computes
    the exact median in one call with no accuracy parameter.

    The value is returned as a Python ``int`` because the domain is discrete
    (sweets are countable) and Galton's own reports were integer pounds.
    """
    row = guesses.agg(F.median(InputColumns.GUESS).alias("crowd_median")).collect()[0]
    return int(round(float(row["crowd_median"])))


# ---------------------------------------------------------------------------
# Stage 8 — Error against ground truth
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CrowdError:
    """Signed error of the crowd's estimate against the truth.

    Galton reported his error the same way: *"the vox populi was in this
    case 9 lb., or 0.8 per cent, of the whole weight too high"*
    (Nature 75:450). We preserve the sign so "too high" and "too low" are
    distinguishable.
    """

    crowd_estimate: int
    true_count: int
    signed_error: int
    signed_percent_error: float


def score_crowd(crowd_estimate: int, true_count: int) -> CrowdError:
    """Compute the crowd's signed error vs the true count."""
    if true_count <= 0:
        raise ValueError(f"true_count must be positive, got {true_count!r}")
    signed_error = crowd_estimate - true_count
    signed_percent_error = 100.0 * signed_error / true_count
    return CrowdError(
        crowd_estimate=crowd_estimate,
        true_count=true_count,
        signed_error=signed_error,
        signed_percent_error=signed_percent_error,
    )


# ---------------------------------------------------------------------------
# Stage 9 — Distribution (Galton's centile table, condensed)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Distribution:
    """A miniature version of Galton's own centile table (Nature 75:450)."""

    n: int
    minimum: int
    q1: int
    median: int
    q3: int
    maximum: int
    iqr: int


def summarise_distribution(guesses: DataFrame) -> Distribution:
    """Compute the five-number summary of the guess column."""
    stats_row = guesses.agg(
        F.count(InputColumns.GUESS).alias("n"),
        F.min(InputColumns.GUESS).alias("min"),
        F.percentile(InputColumns.GUESS, 0.25).alias("q1"),
        F.median(InputColumns.GUESS).alias("median"),
        F.percentile(InputColumns.GUESS, 0.75).alias("q3"),
        F.max(InputColumns.GUESS).alias("max"),
    ).collect()[0]

    q1 = int(round(float(stats_row["q1"])))
    q3 = int(round(float(stats_row["q3"])))
    return Distribution(
        n=int(stats_row["n"]),
        minimum=int(stats_row["min"]),
        q1=q1,
        median=int(round(float(stats_row["median"]))),
        q3=q3,
        maximum=int(stats_row["max"]),
        iqr=q3 - q1,
    )


# ---------------------------------------------------------------------------
# Stage 10 — Persist the summary
# ---------------------------------------------------------------------------


def build_summary_row(
    spark: SparkSession,
    experiment_id: str,
    error: CrowdError,
    distribution: Distribution,
    n_input: int,
    n_dropped: int,
) -> DataFrame:
    """Turn all the numbers into a one-row DataFrame ready to be written."""
    schema = StructType(
        [
            StructField("experiment_id", StringType(), nullable=False),
            StructField("n_input", IntegerType(), nullable=False),
            StructField("n_dropped", IntegerType(), nullable=False),
            StructField("n_used", IntegerType(), nullable=False),
            StructField("crowd_estimate", IntegerType(), nullable=False),
            StructField("true_count", IntegerType(), nullable=False),
            StructField("signed_error", IntegerType(), nullable=False),
            StructField("signed_percent_error", StringType(), nullable=False),
            StructField("min_guess", IntegerType(), nullable=False),
            StructField("q1", IntegerType(), nullable=False),
            StructField("q3", IntegerType(), nullable=False),
            StructField("max_guess", IntegerType(), nullable=False),
            StructField("iqr", IntegerType(), nullable=False),
        ]
    )
    row = (
        experiment_id,
        n_input,
        n_dropped,
        distribution.n,
        error.crowd_estimate,
        error.true_count,
        error.signed_error,
        f"{error.signed_percent_error:+.2f}%",
        distribution.minimum,
        distribution.q1,
        distribution.q3,
        distribution.maximum,
        distribution.iqr,
    )
    return spark.createDataFrame([row], schema=schema)
