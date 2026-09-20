"""ECB Survey of Professional Forecasters ingestion (individual microdata).

Downloads the ZIP of per-round CSVs the ECB publishes at [1]. The archive
holds one ``<YEAR>Q<QUARTER>.csv`` per survey round (1999Q1 onwards), and
each CSV is a stack of *blocks* — one per survey variable — laid out as::

    INFLATION EXPECTATIONS; YEAR-ON-YEAR CHANGE IN HICP,,,,,,
    TARGET_PERIOD,FCT_SOURCE,POINT,TN0_8,FN0_7TN0_3,...
    2026,1,2.5,0,0,0,...
    2027Jun,1,2.1,0,0,0,...

``FCT_SOURCE`` is the anonymised forecaster id, ``POINT`` the point forecast,
and the remaining columns are that forecaster's probability distribution over
outcome bins. We read ``POINT`` only; the bins are a different question shape
and would need their own silver mapping.

Target periods and the quarterly grain
--------------------------------------
Unlike the Philadelphia Fed's SPF (see :mod:`~.spf`, whose horizons are a
tidy 1-6 quarters), the ECB states each variable's rolling horizon in that
variable's *natural reporting frequency*, and mixes it with fixed calendar
targets in the same block:

======  ========================  ==========================
key     rolling target periods    horizons seen, 2020 onward
======  ========================  ==========================
HICP    monthly — ``2027Jun``     3 and 7 quarters
CORE    monthly — ``2027Jun``     3 and 7 quarters
RGDP    quarterly — ``2027Q1``    2 and 6 quarters
UNEM    monthly — ``2027May``     3 and 7 quarters
======  ========================  ==========================

So we normalise every rolling target to the calendar quarter it falls in
(``2027Jun`` → ``2027Q2``) and key rows on that quarter. The fold is lossless
in practice: a variable never has two rolling targets inside one quarter of
the same round, so no two target periods collide into one ``market_id``
(verified across every round from 1999Q1 to 2026Q3).

Calendar-year targets (``2026``) and the long-term target (``2031``) carry no
single quarter, so they fall outside this row grain and are skipped. They are
the bulk of the raw rows; picking them up means a second row grain and its own
``market_id`` scheme, which this source deliberately leaves alone.

Emits one ``numeric_guesses`` row per ``(variable, survey_round, target
quarter)``, with ``guesses`` holding every forecaster's point forecast for
that cell.

References:
  [1] https://www.ecb.europa.eu/stats/ecb_surveys/survey_of_professional_forecasters/html/index.en.html
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.answers import INGEST_ANSWER_SCHEMA, QuestionType

_log = logging.getLogger(__name__)

_DEFAULT_URL = (
    "https://www.ecb.europa.eu/stats/prices/indic/forecast/shared/files/"
    "SPF_individual_forecasts.zip"
)

_USER_AGENT = "wisdom-of-crowds/0.1"
_TIMEOUT_S = 180
_MAX_ATTEMPTS = 4
_BACKOFF_S = 2.0

#: Local-file-header magic, which every non-empty ZIP opens with. The landing
#: page offers the microdata as plain CSV as well as this archive, so we sniff
#: the payload rather than trust the URL's extension.
_ZIP_MAGIC = b"PK\x03\x04"


@dataclass(frozen=True)
class _Variable:
    """One survey variable: how to find its block and how to describe it."""

    key: str         #: config-facing key, e.g. "HICP"
    title: str       #: uppercased prefix of the block's title line
    label: str       #: human phrasing for the question text

    @property
    def slug(self) -> str:
        """Lowercased key, used inside ``market_id``."""
        return self.key.lower()


_VARIABLES: tuple[_Variable, ...] = (
    _Variable("HICP", "INFLATION EXPECTATIONS; YEAR-ON-YEAR CHANGE IN HICP", "HICP inflation"),
    _Variable("CORE", "CORE INFLATION EXPECTATIONS", "core HICP inflation"),
    _Variable("RGDP", "GROWTH EXPECTATIONS", "real GDP growth"),
    _Variable("UNEM", "EXPECTED UNEMPLOYMENT RATE", "unemployment rate"),
)

_VARIABLES_BY_KEY = {v.key: v for v in _VARIABLES}

_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

_ROUND_FILENAME = re.compile(r"(\d{4})Q([1-4])\.csv$", re.IGNORECASE)
_TARGET_QUARTER = re.compile(r"^(\d{4})[Qq]([1-4])$")
_TARGET_MONTH = re.compile(r"^(\d{4})([A-Z][a-z]{2})$")

_TARGET_PERIOD_HEADER = "TARGET_PERIOD"
_POINT_COLUMN = "POINT"


class ECBSPFSource(Source):
    slug = "ecb_spf"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        url = cfg.params.get("download_url", _DEFAULT_URL)
        variables = list(cfg.params.get("variables", ["HICP", "RGDP", "UNEM"]))
        min_year = int(cfg.params.get("min_year", 2020))
        max_horizon = int(cfg.params.get("max_horizon", 6))

        _log.info("ecb_spf_download_start", extra={"url": url})
        raw = self._download(url)
        if raw is None:
            _log.warning("ecb_spf_download_failed", extra={"url": url})
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        wanted = self._resolve_variables(variables)
        rows = list(self._rows_from_archive(raw, wanted, min_year, max_horizon, cfg))
        if not rows:
            _log.warning("ecb_spf_no_rows", extra={
                "url": url, "min_year": min_year, "max_horizon": max_horizon,
            })
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        _log.info("ecb_spf_rows_built", extra={"rows": len(rows)})
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    # ------------------------------------------------------------------
    # download
    # ------------------------------------------------------------------

    @staticmethod
    def _download(url: str) -> bytes | None:
        """Fetch ``url``, retrying transient failures with exponential backoff.

        Returns the payload, or ``None`` once the attempts are spent — callers
        turn that into an empty batch rather than failing the job.
        """
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as r:
                    return r.read()
            except urllib.error.HTTPError as exc:
                # 4xx other than 429 is a bad request, not bad luck: no retry.
                if exc.code != 429 and 400 <= exc.code < 500:
                    _log.warning("ecb_spf_http_error", extra={
                        "url": url, "status": exc.code, "attempt": attempt,
                    })
                    return None
                _log.warning("ecb_spf_http_retry", extra={
                    "url": url, "status": exc.code, "attempt": attempt,
                })
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                _log.warning("ecb_spf_transport_retry", extra={
                    "url": url, "error": str(exc), "attempt": attempt,
                })

            if attempt < _MAX_ATTEMPTS:
                time.sleep(_BACKOFF_S * (2 ** (attempt - 1)))
        return None

    # ------------------------------------------------------------------
    # parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_variables(keys: Sequence[str]) -> list[_Variable]:
        resolved = []
        for key in keys:
            var = _VARIABLES_BY_KEY.get(str(key).strip().upper())
            if var is None:
                _log.warning("ecb_spf_unknown_variable", extra={
                    "variable": key, "known": sorted(_VARIABLES_BY_KEY),
                })
                continue
            resolved.append(var)
        return resolved

    @classmethod
    def _rows_from_archive(
        cls,
        raw: bytes,
        variables: Sequence[_Variable],
        min_year: int,
        max_horizon: int,
        cfg: SourceConfig,
    ) -> Iterator[tuple]:
        """Yield silver tuples for every CSV in the payload."""
        if not variables:
            return
        for filename, text in cls._iter_csvs(raw):
            match = _ROUND_FILENAME.search(filename)
            if match is None:
                # Read-me files and anything else that is not a survey round.
                _log.debug("ecb_spf_skip_file", extra={"filename": filename})
                continue
            survey_year, survey_quarter = int(match.group(1)), int(match.group(2))
            if survey_year < min_year:
                continue
            yield from cls._rows_from_round(
                text, survey_year, survey_quarter, variables, max_horizon, cfg
            )

    @staticmethod
    def _iter_csvs(raw: bytes) -> Iterator[tuple[str, str]]:
        """Yield ``(filename, text)`` for a ZIP of CSVs or a single bare CSV."""
        if not raw.startswith(_ZIP_MAGIC):
            yield ("", raw.decode("utf-8", errors="replace"))
            return
        try:
            archive = zipfile.ZipFile(io.BytesIO(raw))
        except zipfile.BadZipFile:
            _log.warning("ecb_spf_bad_zip")
            return
        with archive:
            for name in sorted(archive.namelist()):
                if name.endswith("/"):
                    continue
                yield (name, archive.read(name).decode("utf-8", errors="replace"))

    @classmethod
    def _rows_from_round(
        cls,
        text: str,
        survey_year: int,
        survey_quarter: int,
        variables: Sequence[_Variable],
        max_horizon: int,
        cfg: SourceConfig,
    ) -> Iterator[tuple]:
        """Walk one survey round's CSV, emitting one row per populated
        (variable, target quarter, forecaster) POINT cell."""
        wanted = {v.key: v for v in variables}
        survey_round = f"{survey_year}Q{survey_quarter}"

        current: _Variable | None = None
        in_point_block = False

        for record in csv.reader(io.StringIO(text)):
            if not record or not any(field.strip() for field in record):
                continue
            head = record[0].strip()

            if head == _TARGET_PERIOD_HEADER:
                # Only blocks carrying a POINT column hold point forecasts;
                # the ASSUMPTIONS block instead has IR / OIL / USD / LAB.
                in_point_block = len(record) > 2 and record[2].strip() == _POINT_COLUMN
                continue

            if head and not head[0].isdigit():
                current = cls._match_variable(head, wanted)
                in_point_block = False
                continue

            if not (in_point_block and current is not None):
                continue

            target = _to_quarter(head)
            if target is None:
                continue  # calendar-year or long-term target: different grain.
            target_year, target_quarter = target
            horizon = _quarter_index(target_year, target_quarter) - _quarter_index(
                survey_year, survey_quarter
            )
            if not 0 <= horizon <= max_horizon:
                continue

            forecaster = record[1].strip() if len(record) > 1 else ""
            point = _to_float(record[2] if len(record) > 2 else "")
            if point is None:
                continue  # forecaster answered the bins but not the point.

            variable = _VARIABLES_BY_KEY[current.key]
            target_str = f"{target_year}Q{target_quarter}"

            yield (
                ECBSPFSource.slug,                                          # source
                f"ecb_spf:{variable.slug}:{survey_round}:{target_str}",      # market_id
                f"ECB SPF: {variable.label}, target {target_str} "
                f"(surveyed {survey_round})",                                # question
                QuestionType.NUMERIC_GUESSES,                                # question_type
                forecaster or None,                                          # name (FCT_SOURCE)
                point,                                                       # answer_value
                None,                                                        # answer_outcome
                None,                                                        # weight
                None,                                                        # created_at
                cfg.cycle_ts,                                                # cycle_ts
            )

    @staticmethod
    def _match_variable(title: str, wanted: dict[str, _Variable]) -> _Variable | None:
        """Return the requested variable whose block this title line opens.

        Matched against every known variable, not just the requested ones, so
        that an unrequested block still clears the previous block's state
        instead of silently absorbing its rows.
        """
        upper = title.upper()
        for var in _VARIABLES:
            if upper.startswith(var.title):
                return wanted.get(var.key)
        return None


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _to_quarter(target_period: str) -> tuple[int, int] | None:
    """Map an ECB target period to the calendar quarter it falls in.

    ``"2027Q1"`` → ``(2027, 1)`` and ``"2027Jun"`` → ``(2027, 2)``. Returns
    ``None`` for calendar-year (``"2026"``) and unrecognised periods, which
    carry no single quarter.
    """
    quarterly = _TARGET_QUARTER.match(target_period)
    if quarterly:
        return int(quarterly.group(1)), int(quarterly.group(2))

    monthly = _TARGET_MONTH.match(target_period)
    if monthly:
        month = _MONTHS.get(monthly.group(2))
        if month is not None:
            return int(monthly.group(1)), (month - 1) // 3 + 1
    return None


def _quarter_index(year: int, quarter: int) -> int:
    """Absolute quarter number, so horizons are a subtraction. 1-indexed quarters."""
    return year * 4 + (quarter - 1)


def _to_float(value: str) -> float | None:
    """Parse a POINT cell, returning ``None`` when the forecaster left it blank."""
    try:
        return float(value.strip())
    except (AttributeError, ValueError):
        return None
