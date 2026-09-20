"""Unit tests for the NOAA GEFS ensemble source.

The interesting behaviour here is the reshaping of Open-Meteo's
"one series per ensemble member" payload into one ``numeric_guesses`` row
whose ``guesses`` array *is* the crowd. These tests run against a recorded
payload shape, so they never touch the network.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession

from wisdom_of_crowds.ingest.base import SourceConfig
from wisdom_of_crowds.ingest.sources import noaa
from wisdom_of_crowds.ingest.sources.noaa import NOAASource
from wisdom_of_crowds.schema.guesses import INGEST_SCHEMA, QuestionType

CYCLE_TS = dt.datetime(2026, 9, 20, 6, 30, tzinfo=dt.UTC)
BASE_DAY = "2026-09-20"
N_MEMBERS = 30  # perturbed members; GEFS adds 1 control run on top


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A single local SparkSession for the whole test run."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("wisdom-of-crowds-noaa-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _payload(
    *,
    hours: int = 200,
    variables: tuple[str, ...] = ("temperature_2m",),
    n_members: int = N_MEMBERS,
) -> dict:
    """An Open-Meteo ensemble payload shaped exactly like the live one.

    Member *m* at hour *h* reports ``h + m/100`` so every value is uniquely
    traceable back to the member and hour it came from.
    """
    start = dt.datetime.fromisoformat(f"{BASE_DAY}T00:00")
    times = [(start + dt.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M") for h in range(hours)]

    hourly: dict = {"time": times}
    units: dict = {"time": "iso8601"}
    for var in variables:
        units[var] = "°C" if var.startswith("temperature") else "mm"
        hourly[var] = [float(h) for h in range(hours)]  # control run
        for m in range(1, n_members + 1):
            hourly[f"{var}_member{m:02d}"] = [h + m / 100 for h in range(hours)]

    return {"latitude": 51.5, "longitude": -0.25, "hourly_units": units, "hourly": hourly}


def _cfg(**params) -> SourceConfig:
    return SourceConfig(slug="noaa", cycle_ts=CYCLE_TS, params=params)


def _rows(payload: dict, *, variables=("temperature_2m",), hours=(24, 72), **kwargs) -> list[dict]:
    """Run the pure reshaping step and return rows as name-keyed dicts."""
    tuples = list(NOAASource._rows_from_payloads(
        [payload],
        [{"name": "London", "lat": 51.5, "lon": -0.13}],
        list(variables),
        list(hours),
        _cfg(),
        **kwargs,
    ))
    return [dict(zip(INGEST_SCHEMA.fieldNames(), t, strict=True)) for t in tuples]


# ---------------------------------------------------------------------------
# The crowd
# ---------------------------------------------------------------------------


def test_every_ensemble_member_becomes_one_guess():
    """The whole point: 30 perturbed members + 1 control = 31 voices."""
    rows = _rows(_payload(), hours=(72,))

    assert len(rows) == 1
    assert len(rows[0]["guesses"]) == N_MEMBERS + 1


def test_guesses_are_the_values_at_the_requested_forecast_hour():
    """Hour 72 must read column 72, not column 0 and not 'now + 72'."""
    rows = _rows(_payload(), hours=(72,))

    guesses = rows[0]["guesses"]
    assert guesses[0] == 72.0                                  # control run
    assert guesses[1:] == [72 + m / 100 for m in range(1, N_MEMBERS + 1)]


def test_control_run_can_be_excluded():
    rows = _rows(_payload(), hours=(24,), include_control=False)

    assert len(rows[0]["guesses"]) == N_MEMBERS
    assert 24.0 not in rows[0]["guesses"]  # the control value is gone


def test_members_that_did_not_report_are_dropped_not_zeroed():
    """A null member must shrink the crowd, never enter it as 0.0."""
    payload = _payload()
    payload["hourly"]["temperature_2m_member03"][24] = None

    rows = _rows(payload, hours=(24,))

    assert len(rows[0]["guesses"]) == N_MEMBERS  # 31 - 1 dropout
    assert 24.03 not in rows[0]["guesses"]


def test_trader_count_records_how_many_members_reported():
    payload = _payload()
    payload["hourly"]["temperature_2m_member01"][24] = None

    rows = _rows(payload, hours=(24,))

    assert rows[0]["trader_count"] == len(rows[0]["guesses"]) == N_MEMBERS


# ---------------------------------------------------------------------------
# Row identity and shape
# ---------------------------------------------------------------------------


def test_row_matches_the_ingest_contract():
    rows = _rows(_payload(), hours=(72,))
    row = rows[0]

    assert row["source"] == "noaa"                      # the slug, not the int id
    assert row["question_type"] == QuestionType.NUMERIC_GUESSES
    assert row["is_resolved"] is False                  # a future forecast
    assert row["end_date"] == dt.datetime(2026, 9, 23, 0, 0, tzinfo=dt.UTC)
    assert row["cycle_ts"] == CYCLE_TS
    assert row["outcomes"] is None
    assert row["prices"] is None
    assert row["resolved_value"] is None


def test_market_id_is_deterministic_and_readable():
    rows = _rows(_payload(), hours=(72,))

    assert rows[0]["market_id"] == "gefs:temp_2m:london:2026-09-23T00Z:t+72h"


def test_question_reads_as_a_sentence():
    rows = _rows(_payload(), hours=(72,))

    assert rows[0]["question"] == (
        "GEFS ensemble: 2m temperature (C) at London on 2026-09-23 00Z (72h out)"
    )


def test_one_row_per_location_variable_horizon():
    rows = _rows(
        _payload(variables=("temperature_2m", "precipitation")),
        variables=("temperature_2m", "precipitation"),
        hours=(24, 72, 168),
    )

    assert len(rows) == 2 * 3
    assert len({r["market_id"] for r in rows}) == 6  # every id distinct


def test_ids_are_stable_across_runs():
    """Same forecast target, different cycle -> same market_id, so a re-run
    truncate-loads instead of duplicating."""
    first = _rows(_payload(), hours=(24,))
    second = _rows(_payload(), hours=(24,))

    assert first[0]["market_id"] == second[0]["market_id"]


# ---------------------------------------------------------------------------
# Degrading rather than exploding
# ---------------------------------------------------------------------------


def test_horizon_beyond_the_series_is_skipped():
    """168h asked of a 48h payload yields no row, and no exception."""
    rows = _rows(_payload(hours=48), hours=(24, 168))

    assert [r["market_id"] for r in rows] == ["gefs:temp_2m:london:2026-09-21T00Z:t+24h"]


def test_unknown_variable_is_skipped():
    rows = _rows(_payload(), variables=("temperature_2m", "not_a_real_variable"), hours=(24,))

    assert len(rows) == 1


def test_payload_without_hourly_block_is_skipped():
    assert _rows({"hourly": {}}, hours=(24,)) == []


def test_unmapped_variable_still_gets_a_slug_and_label():
    payload = _payload(variables=("shortwave_radiation",))
    rows = _rows(payload, variables=("shortwave_radiation",), hours=(24,))

    assert rows[0]["market_id"].startswith("gefs:shortwave_radiation:london:")
    assert "shortwave radiation" in rows[0]["question"]


# ---------------------------------------------------------------------------
# extract(): Spark boundary
# ---------------------------------------------------------------------------


def test_extract_returns_ingest_schema_dataframe(spark, monkeypatch):
    monkeypatch.setattr(NOAASource, "_fetch", classmethod(lambda cls, *a, **k: [_payload()]))

    df = NOAASource().extract(spark, _cfg(variables=["temperature_2m"], forecast_hours=[24, 72]))

    assert df.schema == INGEST_SCHEMA
    assert df.count() == 2
    assert df.first()["source"] == "noaa"


def test_failed_download_is_an_empty_batch_not_a_raise(spark, monkeypatch):
    """Hard requirement: a dead upstream must not fail the run."""
    def _boom(cls, *a, **k):
        raise RuntimeError("upstream is down")

    monkeypatch.setattr(NOAASource, "_fetch", classmethod(_boom))

    df = NOAASource().extract(spark, _cfg())

    assert df.count() == 0
    assert df.schema == INGEST_SCHEMA


def test_empty_config_is_an_empty_batch(spark):
    df = NOAASource().extract(spark, _cfg(locations=[], variables=[], forecast_hours=[]))

    assert df.count() == 0
    assert df.schema == INGEST_SCHEMA


# ---------------------------------------------------------------------------
# HTTP retry
# ---------------------------------------------------------------------------


def test_get_json_retries_then_succeeds(monkeypatch):
    import urllib.error

    calls = {"n": 0}

    class _Response:
        def read(self):
            return b'{"ok": true}'

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _flaky(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.URLError("transient")
        return _Response()

    monkeypatch.setattr(noaa.urllib.request, "urlopen", _flaky)
    monkeypatch.setattr(noaa.time, "sleep", lambda _s: None)

    assert NOAASource._get_json("https://example.invalid/x", retries=3) == {"ok": True}
    assert calls["n"] == 3


def test_get_json_raises_after_exhausting_retries(monkeypatch):
    import urllib.error

    def _always_fail(req, timeout=None):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(noaa.urllib.request, "urlopen", _always_fail)
    monkeypatch.setattr(noaa.time, "sleep", lambda _s: None)

    # extract() is what swallows this; _get_json itself is allowed to raise.
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        NOAASource._get_json("https://example.invalid/x", retries=3)
