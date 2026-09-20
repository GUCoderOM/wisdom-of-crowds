"""Unit tests for the AAII Investor Sentiment source.

These exercise the pure parsing / row-shaping helpers and the download
retry policy. None of them need Spark or the network: the source keeps its
Spark import behind ``TYPE_CHECKING`` and its row builder returns plain
tuples, so the interesting logic is directly callable.
"""

from __future__ import annotations

import datetime as dt
import urllib.error

import pytest

from wisdom_of_crowds.ingest.base import SourceConfig
from wisdom_of_crowds.ingest.sources import aaii
from wisdom_of_crowds.schema.silver import SILVER_SCHEMA, QuestionType, SilverColumns

CYCLE_TS = dt.datetime(2026, 9, 20, 12, 0, 0, tzinfo=dt.UTC)

# Two real survey Thursdays, in FRED's CSV shape: a BOM, a header naming the
# series, and one `.` standing in for a missing observation.
BULLISH_CSV = (
    "﻿observation_date,BULLISH\n"
    "2019-12-26,33.1\n"
    "2026-09-10,28.8\n"
    "2026-09-17,30.0\n"
)
BEARISH_CSV = (
    "observation_date,BEARISH\n"
    "2019-12-26,21.2\n"
    "2026-09-10,53.3\n"
    "2026-09-17,50.0\n"
)
NEUTRAL_CSV = (
    "observation_date,NEUTRAL\n"
    "2019-12-26,45.7\n"
    "2026-09-10,17.9\n"
    "2026-09-17,.\n"
)


@pytest.fixture
def cfg() -> SourceConfig:
    return SourceConfig(source_id="aaii", cycle_ts=CYCLE_TS, params={})


class _FakeSpark:
    """Records what the source hands to ``createDataFrame``."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def createDataFrame(self, rows, schema):  # noqa: N802 - mirrors the Spark API
        rows = list(rows)
        self.calls.append((rows, schema))
        return rows


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def test_parses_dates_and_values_skipping_header_and_bom() -> None:
    parsed = aaii._parse_series_csv(BULLISH_CSV)
    assert parsed == {
        dt.date(2019, 12, 26): 33.1,
        dt.date(2026, 9, 10): 28.8,
        dt.date(2026, 9, 17): 30.0,
    }


def test_missing_observation_is_dropped_not_zeroed() -> None:
    """FRED writes `.` for a gap; reading it as 0.0 would invent a survey."""
    assert dt.date(2026, 9, 17) not in aaii._parse_series_csv(NEUTRAL_CSV)


def test_value_column_name_is_irrelevant() -> None:
    """Repointing at another provider must not need a code change."""
    parsed = aaii._parse_series_csv("Date,Percent Bullish\n2026-09-10,28.8\n")
    assert parsed == {dt.date(2026, 9, 10): 28.8}


# ---------------------------------------------------------------------------
# Row shaping
# ---------------------------------------------------------------------------


def _observations() -> dict[str, dict[dt.date, float]]:
    return {
        "bullish": aaii._parse_series_csv(BULLISH_CSV),
        "bearish": aaii._parse_series_csv(BEARISH_CSV),
        "neutral": aaii._parse_series_csv(NEUTRAL_CSV),
    }


def test_one_row_per_week_carried_by_every_series() -> None:
    """2019-12-26 is before min_date; 2026-09-17 has no neutral reading."""
    rows = list(aaii._silver_rows(_observations(), dt.date(2020, 1, 1), cfg=_cfg()))
    assert len(rows) == 1


def test_row_matches_the_silver_schema_positionally() -> None:
    (row,) = list(aaii._silver_rows(_observations(), dt.date(2020, 1, 1), cfg=_cfg()))
    named = dict(zip(SILVER_SCHEMA.fieldNames(), row, strict=True))

    assert named[SilverColumns.SOURCE] == "aaii"
    assert named[SilverColumns.MARKET_ID] == "aaii:2026-09-10"
    assert named[SilverColumns.QUESTION] == (
        "AAII Investor Sentiment: bullish vs bearish vs neutral (week of 2026-09-10)"
    )
    assert named[SilverColumns.QUESTION_TYPE] == QuestionType.POLL_CATEGORICAL
    assert named[SilverColumns.OUTCOMES] == ["Bullish", "Bearish", "Neutral"]
    assert named[SilverColumns.PRICES] == pytest.approx([0.288, 0.533, 0.179])
    assert named[SilverColumns.GUESSES] is None
    assert named[SilverColumns.TRADER_COUNT] is None
    assert named[SilverColumns.IS_RESOLVED] is False
    assert named[SilverColumns.CYCLE_TS] == CYCLE_TS


def test_prices_are_fractions_summing_to_about_one() -> None:
    (row,) = list(aaii._silver_rows(_observations(), dt.date(2020, 1, 1), cfg=_cfg()))
    prices = dict(zip(SILVER_SCHEMA.fieldNames(), row, strict=True))[SilverColumns.PRICES]
    assert sum(prices) == pytest.approx(1.0, abs=0.01)


def test_prices_line_up_with_outcomes() -> None:
    """Bearish led that week — the ordering must survive the join."""
    (row,) = list(aaii._silver_rows(_observations(), dt.date(2020, 1, 1), cfg=_cfg()))
    named = dict(zip(SILVER_SCHEMA.fieldNames(), row, strict=True))
    pairs = dict(zip(named[SilverColumns.OUTCOMES], named[SilverColumns.PRICES], strict=True))
    assert max(pairs, key=pairs.__getitem__) == "Bearish"


def test_min_date_filters_older_weeks() -> None:
    rows = list(aaii._silver_rows(_observations(), dt.date(2019, 1, 1), cfg=_cfg()))
    market_ids = [r[1] for r in rows]
    assert market_ids == ["aaii:2019-12-26", "aaii:2026-09-10"]


def test_weeks_are_emitted_oldest_first() -> None:
    rows = list(aaii._silver_rows(_observations(), dt.date(2019, 1, 1), cfg=_cfg()))
    assert rows == sorted(rows, key=lambda r: r[1])


# ---------------------------------------------------------------------------
# Download policy
# ---------------------------------------------------------------------------


def test_permanent_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 404 is a settled answer — this is what FRED returns for BULLISH."""
    attempts = _count_attempts(monkeypatch, _raising(404))
    assert aaii.AAIISource._fetch_csv(aaii._DEFAULT_BASE_URL, "BULLISH") is None
    assert attempts == [1]


def test_transient_error_is_retried_then_given_up_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = _count_attempts(monkeypatch, _raising(503))
    assert aaii.AAIISource._fetch_csv(aaii._DEFAULT_BASE_URL, "BULLISH") is None
    assert len(attempts) == aaii._MAX_ATTEMPTS


def test_transient_error_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def flaky(req, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            raise urllib.error.URLError("connection reset")
        return _Response(b"observation_date,BULLISH\n2026-09-10,28.8\n")

    monkeypatch.setattr(aaii.time, "sleep", lambda _s: None)
    monkeypatch.setattr(aaii.urllib.request, "urlopen", flaky)

    body = aaii.AAIISource._fetch_csv(aaii._DEFAULT_BASE_URL, "BULLISH")
    assert body is not None
    assert aaii._parse_series_csv(body) == {dt.date(2026, 9, 10): 28.8}
    assert len(calls) == 2


def test_download_failure_yields_empty_batch_not_an_exception(
    monkeypatch: pytest.MonkeyPatch, cfg: SourceConfig
) -> None:
    """Rule of the house: a bad upstream day must not fail the ingest run."""
    monkeypatch.setattr(aaii.AAIISource, "_fetch_csv", staticmethod(lambda *a: None))
    spark = _FakeSpark()

    result = aaii.AAIISource().extract(spark, cfg)

    assert result == []
    (rows, schema), = spark.calls
    assert rows == []
    assert schema is SILVER_SCHEMA


def test_extract_builds_rows_from_the_three_series(
    monkeypatch: pytest.MonkeyPatch, cfg: SourceConfig
) -> None:
    bodies = {"BULLISH": BULLISH_CSV, "BEARISH": BEARISH_CSV, "NEUTRAL": NEUTRAL_CSV}
    monkeypatch.setattr(
        aaii.AAIISource, "_fetch_csv", staticmethod(lambda _base, sid: bodies[sid])
    )
    spark = _FakeSpark()

    rows = aaii.AAIISource().extract(spark, cfg)

    assert [r[1] for r in rows] == ["aaii:2026-09-10"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _cfg() -> SourceConfig:
    return SourceConfig(source_id="aaii", cycle_ts=CYCLE_TS, params={})


class _Response:
    """The bare minimum of the context-manager urlopen() hands back."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def _raising(status: int):
    def _open(req, timeout=None):
        raise urllib.error.HTTPError(
            url="https://example.test", code=status, msg="nope", hdrs=None, fp=None
        )

    return _open


def _count_attempts(monkeypatch: pytest.MonkeyPatch, opener) -> list[int]:
    attempts: list[int] = []

    def counting(req, timeout=None):
        attempts.append(len(attempts) + 1)
        return opener(req, timeout=timeout)

    monkeypatch.setattr(aaii.time, "sleep", lambda _s: None)
    monkeypatch.setattr(aaii.urllib.request, "urlopen", counting)
    return attempts
