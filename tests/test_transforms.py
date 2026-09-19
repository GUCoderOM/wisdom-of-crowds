"""Unit tests for the pure transforms.

Every test here should be readable as an executable statement of Galton's
method. If a test starts describing implementation detail rather than
experimental behaviour, rewrite it.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from wisdom_of_crowds.columns import FlagColumns
from wisdom_of_crowds.defects import DefectReason
from wisdom_of_crowds.schema import SchemaValidationError
from wisdom_of_crowds.transforms import (
    CleanResult,
    CrowdError,
    Distribution,
    clean_guesses,
    crowd_median,
    flag_defective,
    score_crowd,
    summarise_distribution,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A single local SparkSession for the whole test run."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("wisdom-of-crowds-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_guesses(spark: SparkSession):
    """A small hand-crafted crowd with a known median (=100)."""
    return spark.createDataFrame(
        [
            ("Alice", 50),
            ("Bob", 90),
            ("Carol", 100),
            ("Dan", 110),
            ("Eve", 500),  # a fat-tail voter — Galton kept these
        ],
        schema="name string, guess int",
    )


# ---------------------------------------------------------------------------
# Stage 5 — flag_defective
# ---------------------------------------------------------------------------


def _row_reasons(row) -> set[str]:
    return set(row[FlagColumns.DEFECT_REASONS] or [])


def test_flag_defective_marks_missing_name(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("ok", 42), (None, 42), ("", 42), ("   ", 42)],
        schema="name string, guess int",
    )
    rows = flag_defective(df).collect()
    assert _row_reasons(rows[0]) == set()
    assert _row_reasons(rows[1]) == {DefectReason.MISSING_NAME.value}
    assert _row_reasons(rows[2]) == {DefectReason.MISSING_NAME.value}
    assert _row_reasons(rows[3]) == {DefectReason.MISSING_NAME.value}


def test_flag_defective_marks_missing_guess(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("ok", 42), ("blank", None)],
        schema="name string, guess int",
    )
    reasons = [_row_reasons(row) for row in flag_defective(df).collect()]
    assert reasons == [set(), {DefectReason.MISSING_GUESS.value}]


def test_flag_defective_marks_non_positive_guess(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("neg", -5), ("zero", 0), ("one", 1)],
        schema="name string, guess int",
    )
    reasons = {row["name"]: _row_reasons(row) for row in flag_defective(df).collect()}
    assert reasons == {
        "neg":  {DefectReason.NON_POSITIVE_GUESS.value},
        "zero": {DefectReason.NON_POSITIVE_GUESS.value},
        "one":  set(),
    }


def test_flag_defective_reports_multiple_reasons_per_row(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(None, -5)],  # missing name AND non-positive guess
        schema="name string, guess int",
    )
    (row,) = flag_defective(df).collect()
    assert _row_reasons(row) == {
        DefectReason.MISSING_NAME.value,
        DefectReason.NON_POSITIVE_GUESS.value,
    }
    assert row[FlagColumns.IS_DEFECTIVE] is True


def test_flag_defective_rejects_wrong_schema(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 2)], schema="name int, guess int")
    with pytest.raises(SchemaValidationError):
        flag_defective(df)


# ---------------------------------------------------------------------------
# Stage 6 — clean_guesses
# ---------------------------------------------------------------------------


def test_clean_guesses_keeps_only_valid_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("ok1", 10), ("ok2", 20), (None, 30), ("bad", -1)],
        schema="name string, guess int",
    )
    result: CleanResult = clean_guesses(flag_defective(df))
    assert result.n_input == 4
    assert result.n_kept == 2
    assert result.n_dropped == 2
    kept_names = {row["name"] for row in result.kept.collect()}
    assert kept_names == {"ok1", "ok2"}


def test_clean_guesses_drops_flag_columns_from_kept(spark: SparkSession) -> None:
    df = spark.createDataFrame([("ok", 10)], schema="name string, guess int")
    result = clean_guesses(flag_defective(df))
    assert result.kept.columns == ["name", "guess"]


def test_clean_guesses_reports_drops_per_reason(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("ok", 10),
            (None, 5),      # missing_name
            ("blank", None), # missing_guess
            ("neg", -3),     # non_positive_guess
        ],
        schema="name string, guess int",
    )
    result = clean_guesses(flag_defective(df))
    assert result.dropped_by_reason == {
        DefectReason.MISSING_NAME:       1,
        DefectReason.MISSING_GUESS:      1,
        DefectReason.NON_POSITIVE_GUESS: 1,
    }


def test_clean_guesses_counts_multiply_defective_rows_in_every_reason(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [
            (None, -5),  # two reasons; contributes to both counts
        ],
        schema="name string, guess int",
    )
    result = clean_guesses(flag_defective(df))
    assert result.n_dropped == 1
    assert result.dropped_by_reason[DefectReason.MISSING_NAME] == 1
    assert result.dropped_by_reason[DefectReason.NON_POSITIVE_GUESS] == 1
    # And the sum of per-reason drops correctly exceeds n_dropped:
    assert sum(result.dropped_by_reason.values()) == 2
    assert result.n_dropped == 1


def test_clean_guesses_derives_n_kept_consistently(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("ok1", 1), ("ok2", 2), ("ok3", 3), (None, 4), ("bad", 0)],
        schema="name string, guess int",
    )
    result = clean_guesses(flag_defective(df))
    assert result.n_kept == result.n_input - result.n_dropped


# ---------------------------------------------------------------------------
# Stage 7 — crowd_median
# ---------------------------------------------------------------------------


def test_crowd_median_is_the_middlemost_estimate(sample_guesses) -> None:
    # Sorted guesses: 50, 90, 100, 110, 500 -> median 100
    assert crowd_median(sample_guesses) == 100


def test_crowd_median_ignores_fat_tail_influence(spark: SparkSession) -> None:
    """Galton's whole argument for the median: cranks shouldn't dominate."""
    df = spark.createDataFrame(
        [("a", 100), ("b", 100), ("c", 100), ("d", 100), ("e", 10_000_000)],
        schema="name string, guess int",
    )
    assert crowd_median(df) == 100  # a mean would be ~2_000_080


# ---------------------------------------------------------------------------
# Stage 8 — score_crowd
# ---------------------------------------------------------------------------


def test_score_crowd_signed_error_preserves_direction() -> None:
    high = score_crowd(crowd_estimate=1207, true_count=1198)
    low = score_crowd(crowd_estimate=1180, true_count=1198)
    assert high.signed_error == 9
    assert low.signed_error == -18
    assert high.signed_percent_error == pytest.approx(0.751, abs=0.01)
    assert low.signed_percent_error == pytest.approx(-1.503, abs=0.01)


def test_score_crowd_replicates_galton_1907() -> None:
    """The published number from Vox Populi (Nature 75:450)."""
    result: CrowdError = score_crowd(crowd_estimate=1207, true_count=1198)
    # Galton reported "9 lb., or 0.8 per cent, of the whole weight too high".
    assert result.signed_error == 9
    assert result.signed_percent_error == pytest.approx(0.8, abs=0.05)


def test_score_crowd_rejects_non_positive_truth() -> None:
    with pytest.raises(ValueError):
        score_crowd(crowd_estimate=100, true_count=0)


# ---------------------------------------------------------------------------
# Stage 9 — summarise_distribution
# ---------------------------------------------------------------------------


def test_summarise_distribution_returns_five_number_summary(sample_guesses) -> None:
    dist: Distribution = summarise_distribution(sample_guesses)
    assert dist.n == 5
    assert dist.minimum == 50
    assert dist.maximum == 500
    assert dist.median == 100
    assert dist.q1 <= dist.median <= dist.q3
    assert dist.iqr == dist.q3 - dist.q1
