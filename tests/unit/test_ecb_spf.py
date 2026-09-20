"""Unit tests for the ECB SPF source.

The fixtures below are trimmed copies of the real archive's layout — the
block title lines, the ``TARGET_PERIOD,FCT_SOURCE,POINT,<bins>`` header, the
ASSUMPTIONS block that has no POINT column, and the mix of quarterly, monthly
and calendar-year target periods that sit inside one block. Tests state what
the source promises silver, not how it walks the file.
"""

from __future__ import annotations

import datetime as dt
import io
import zipfile

import pytest
from pyspark.sql import SparkSession

from wisdom_of_crowds.ingest.base import SourceConfig
from wisdom_of_crowds.ingest.sources import ecb_spf
from wisdom_of_crowds.ingest.sources.ecb_spf import ECBSPFSource
from wisdom_of_crowds.schema.silver import INGEST_SCHEMA, QuestionType, SilverColumns

CYCLE_TS = dt.datetime(2026, 9, 20, 12, 0, 0)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A single local SparkSession for the whole test run."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("ecb-spf-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# One survey round, 2026Q3, in the real archive's shape. HICP states its
# rolling horizon in months (2027Jun -> 2027Q2), RGDP in quarters, and both
# carry calendar-year targets in the same block.
ROUND_2026Q3 = """INFLATION EXPECTATIONS; YEAR-ON-YEAR CHANGE IN HICP,,,,,
TARGET_PERIOD,FCT_SOURCE,POINT,TN0_8,FN0_7TN0_3,F4_8
2026,1,2.5,0,0,0
2027Jun,1,2.1,0,0,0
2027Jun,5,1.9,0,0,0
2027Jun,7,,0,0,0
2031,1,2.0,0,0,0
GROWTH EXPECTATIONS; YEAR-ON-YEAR CHANGE IN REAL GDP,,,,,
TARGET_PERIOD,FCT_SOURCE,POINT,TN1_0,FN1_0TN0_6,F4_0
2026,1,1.1,0,0,0
2027Q1,1,1.3,0,0,0
2027Q1,5,1.5,0,0,0
EXPECTED UNEMPLOYMENT RATE; PERCENTAGE OF LABOUR FORCE,,,,,
TARGET_PERIOD,FCT_SOURCE,POINT,T4_0,F4_0T4_4,F11_0
2027May,1,6.2,0,0,0
ASSUMPTIONS,,,,,
TARGET_PERIOD,FCT_SOURCE,IR,OIL,USD,LAB
2026Q4,1,2.0,70,1.1,1.0
2027Q1,1,2.0,71,1.1,1.0
"""

# An older round, used to prove min_year filters whole files out.
ROUND_2019Q1 = """INFLATION EXPECTATIONS; YEAR-ON-YEAR CHANGE IN HICP,,,,,
TARGET_PERIOD,FCT_SOURCE,POINT,TN0_8,FN0_7TN0_3,F4_8
2019Q3,1,1.4,0,0,0
"""


def _zip(**files: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, text in files.items():
            z.writestr(name, text)
    return buf.getvalue()


@pytest.fixture
def archive() -> bytes:
    return _zip(**{"2026Q3.csv": ROUND_2026Q3, "2019Q1.csv": ROUND_2019Q1})


def _cfg(**params) -> SourceConfig:
    base = {
        "download_url": "https://example.invalid/spf.zip",
        "variables": ["HICP", "RGDP", "UNEM"],
        "min_year": 2020,
        "max_horizon": 8,
    }
    base.update(params)
    return SourceConfig(source_id="ecb_spf", cycle_ts=CYCLE_TS, params=base)


def _extract(spark, monkeypatch, payload, **params):
    monkeypatch.setattr(ECBSPFSource, "_download", staticmethod(lambda url: payload))
    return ECBSPFSource().extract(spark, _cfg(**params))


def _by_market_id(df) -> dict:
    return {r[SilverColumns.MARKET_ID]: r for r in df.collect()}


# ---------------------------------------------------------------------------
# Row shape
# ---------------------------------------------------------------------------


def test_emits_one_row_per_variable_round_and_target_quarter(spark, monkeypatch, archive):
    rows = _by_market_id(_extract(spark, monkeypatch, archive))

    assert set(rows) == {
        "ecb_spf:hicp:2026Q3:2027Q2",
        "ecb_spf:rgdp:2026Q3:2027Q1",
        "ecb_spf:unem:2026Q3:2027Q2",
    }


def test_guesses_collect_every_forecaster_in_the_cell(spark, monkeypatch, archive):
    row = _by_market_id(_extract(spark, monkeypatch, archive))["ecb_spf:rgdp:2026Q3:2027Q1"]

    assert sorted(row[SilverColumns.GUESSES]) == [1.3, 1.5]


def test_question_names_variable_target_and_survey_round(spark, monkeypatch, archive):
    row = _by_market_id(_extract(spark, monkeypatch, archive))["ecb_spf:hicp:2026Q3:2027Q2"]

    assert row[SilverColumns.QUESTION] == (
        "ECB SPF: HICP inflation, target 2027Q2 (surveyed 2026Q3)"
    )


def test_row_carries_silver_defaults_for_a_numeric_question(spark, monkeypatch, archive):
    row = _by_market_id(_extract(spark, monkeypatch, archive))["ecb_spf:hicp:2026Q3:2027Q2"]

    assert row[SilverColumns.SOURCE] == "ecb_spf"
    assert row[SilverColumns.QUESTION_TYPE] == QuestionType.NUMERIC_GUESSES
    assert row[SilverColumns.OUTCOMES] is None
    assert row[SilverColumns.PRICES] is None
    assert row[SilverColumns.TRADER_COUNT] is None
    assert row[SilverColumns.VOLUME_USD] is None
    assert row[SilverColumns.END_DATE] is None
    assert row[SilverColumns.IS_RESOLVED] is False
    assert row[SilverColumns.RESOLVED_OUTCOME] is None
    assert row[SilverColumns.RESOLVED_VALUE] is None
    assert row[SilverColumns.CYCLE_TS] == CYCLE_TS


def test_output_conforms_to_the_silver_schema(spark, monkeypatch, archive):
    df = _extract(spark, monkeypatch, archive)

    assert df.schema == INGEST_SCHEMA


# ---------------------------------------------------------------------------
# Target periods
# ---------------------------------------------------------------------------


def test_monthly_targets_fold_into_the_quarter_they_fall_in(spark, monkeypatch, archive):
    # 2027Jun is the third month of 2027Q2; 2027May the second.
    rows = _by_market_id(_extract(spark, monkeypatch, archive))

    assert "ecb_spf:hicp:2026Q3:2027Q2" in rows
    assert "ecb_spf:unem:2026Q3:2027Q2" in rows


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("2027Q1", (2027, 1)),
        ("2027Jan", (2027, 1)),
        ("2027Mar", (2027, 1)),
        ("2027Apr", (2027, 2)),
        ("2027Jun", (2027, 2)),
        ("2027Dec", (2027, 4)),
        ("2026", None),       # calendar-year target: no single quarter
        ("2031", None),       # long-term target
        ("2027Foo", None),    # unparseable
        ("", None),
    ],
)
def test_target_period_maps_to_a_calendar_quarter(period, expected):
    assert ecb_spf._to_quarter(period) == expected


def test_calendar_year_targets_are_skipped(spark, monkeypatch, archive):
    # The HICP block holds 2026 and 2031 alongside 2027Jun; only the rolling
    # target has a quarter, so it is the only HICP row.
    hicp = [m for m in _by_market_id(_extract(spark, monkeypatch, archive)) if ":hicp:" in m]

    assert hicp == ["ecb_spf:hicp:2026Q3:2027Q2"]


def test_blank_point_forecasts_are_dropped(spark, monkeypatch, archive):
    # Forecaster 7 filled in the bins but left POINT empty.
    row = _by_market_id(_extract(spark, monkeypatch, archive))["ecb_spf:hicp:2026Q3:2027Q2"]

    assert sorted(row[SilverColumns.GUESSES]) == [1.9, 2.1]


def test_assumptions_block_is_ignored(spark, monkeypatch, archive):
    # ASSUMPTIONS has quarterly targets but no POINT column — its IR values
    # must not be mistaken for forecasts of a survey variable.
    guesses = [
        g
        for row in _extract(spark, monkeypatch, archive).collect()
        for g in row[SilverColumns.GUESSES]
    ]

    assert 70.0 not in guesses  # the OIL column
    assert sorted(guesses) == [1.3, 1.5, 1.9, 2.1, 6.2]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_min_year_drops_earlier_survey_rounds(spark, monkeypatch, archive):
    rows = _by_market_id(_extract(spark, monkeypatch, archive, min_year=2020))

    assert not [m for m in rows if ":2019Q1:" in m]


def test_min_year_can_reach_back_to_older_rounds(spark, monkeypatch, archive):
    rows = _by_market_id(_extract(spark, monkeypatch, archive, min_year=2019))

    assert "ecb_spf:hicp:2019Q1:2019Q3" in rows


def test_max_horizon_clips_distant_targets(spark, monkeypatch, archive):
    # 2027Q2 is 3 quarters past the 2026Q3 round, 2027Q1 is 2.
    rows = _by_market_id(_extract(spark, monkeypatch, archive, max_horizon=2))

    assert set(rows) == {"ecb_spf:rgdp:2026Q3:2027Q1"}


def test_variables_selects_which_blocks_are_read(spark, monkeypatch, archive):
    rows = _by_market_id(_extract(spark, monkeypatch, archive, variables=["RGDP"]))

    assert set(rows) == {"ecb_spf:rgdp:2026Q3:2027Q1"}


def test_unknown_variables_are_skipped_not_fatal(spark, monkeypatch, archive):
    rows = _by_market_id(_extract(spark, monkeypatch, archive, variables=["RGDP", "NOPE"]))

    assert set(rows) == {"ecb_spf:rgdp:2026Q3:2027Q1"}


def test_variable_keys_are_case_insensitive(spark, monkeypatch, archive):
    rows = _by_market_id(_extract(spark, monkeypatch, archive, variables=["rgdp"]))

    assert set(rows) == {"ecb_spf:rgdp:2026Q3:2027Q1"}


# ---------------------------------------------------------------------------
# Payload shapes and failure
# ---------------------------------------------------------------------------


def test_a_bare_csv_payload_is_read_as_one_round(spark, monkeypatch):
    # The landing page also offers the microdata as a single CSV.
    df = _extract(spark, monkeypatch, ROUND_2026Q3.encode("utf-8"))

    # No filename means no survey round to key on, so nothing is emitted —
    # but the source must not blow up on the shape.
    assert df.count() == 0
    assert df.schema == INGEST_SCHEMA


def test_files_that_are_not_survey_rounds_are_skipped(spark, monkeypatch):
    payload = _zip(**{"readme.txt": "notes", "2026Q3.csv": ROUND_2026Q3})

    assert _extract(spark, monkeypatch, payload).count() == 3


def test_download_failure_yields_an_empty_batch(spark, monkeypatch):
    df = _extract(spark, monkeypatch, None)

    assert df.count() == 0
    assert df.schema == INGEST_SCHEMA


def test_corrupt_archive_yields_an_empty_batch(spark, monkeypatch):
    df = _extract(spark, monkeypatch, b"PK\x03\x04not-really-a-zip")

    assert df.count() == 0
    assert df.schema == INGEST_SCHEMA


# ---------------------------------------------------------------------------
# Download retry
# ---------------------------------------------------------------------------


def test_download_retries_transient_failures_then_succeeds(monkeypatch):
    import urllib.error

    attempts = []

    class _Response:
        def read(self) -> bytes:
            return b"payload"

        def __enter__(self):
            return self

        def __exit__(self, *exc) -> bool:
            return False

    def fake_urlopen(req, timeout=None):
        attempts.append(1)
        if len(attempts) < 3:
            raise urllib.error.URLError("connection reset")
        return _Response()

    monkeypatch.setattr(ecb_spf.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(ecb_spf.time, "sleep", lambda _s: None)

    assert ECBSPFSource._download("https://example.invalid/spf.zip") == b"payload"
    assert len(attempts) == 3


def test_download_gives_up_after_the_last_attempt(monkeypatch):
    import urllib.error

    attempts = []

    def fake_urlopen(req, timeout=None):
        attempts.append(1)
        raise urllib.error.URLError("down")

    monkeypatch.setattr(ecb_spf.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(ecb_spf.time, "sleep", lambda _s: None)

    assert ECBSPFSource._download("https://example.invalid/spf.zip") is None
    assert len(attempts) == ecb_spf._MAX_ATTEMPTS


def test_download_does_not_retry_a_client_error(monkeypatch):
    import urllib.error

    attempts = []

    def fake_urlopen(req, timeout=None):
        attempts.append(1)
        raise urllib.error.HTTPError("url", 404, "Not Found", {}, None)

    monkeypatch.setattr(ecb_spf.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(ecb_spf.time, "sleep", lambda _s: None)

    assert ECBSPFSource._download("https://example.invalid/spf.zip") is None
    assert len(attempts) == 1


def test_download_retries_a_rate_limit(monkeypatch):
    import urllib.error

    attempts = []

    def fake_urlopen(req, timeout=None):
        attempts.append(1)
        raise urllib.error.HTTPError("url", 429, "Too Many Requests", {}, None)

    monkeypatch.setattr(ecb_spf.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(ecb_spf.time, "sleep", lambda _s: None)

    assert ECBSPFSource._download("https://example.invalid/spf.zip") is None
    assert len(attempts) == ecb_spf._MAX_ATTEMPTS
