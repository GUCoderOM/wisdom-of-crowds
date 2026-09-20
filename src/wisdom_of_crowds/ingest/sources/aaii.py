"""AAII Investor Sentiment Survey ingestion.

The American Association of Individual Investors has polled its members
weekly since 1987 with a single question: over the next six months, is the
direction of the stock market bullish, bearish or neutral? Results are
published each Thursday as three percentages that sum to 100.

That is a poll, not a market. The public feed carries no per-respondent
detail — only the three aggregate shares — so each survey week becomes one
silver row of ``question_type = 'poll_categorical'`` with ``outcomes`` and
``prices`` populated and ``guesses`` left null. ``trader_count`` is null
too: AAII does not publish the weekly response count.

The three shares arrive as three separate one-column CSV downloads, joined
here on observation date. A week is emitted only when all three series
carry it, since a partial week cannot produce prices summing to ~1.

.. warning::

   As of 2026-09-20 the configured series IDs (``BULLISH``, ``BEARISH``,
   ``NEUTRAL``) return HTTP 404 from FRED, which does not carry the AAII
   survey — the data is AAII's own and licensed. ``fred_base_url`` and
   ``series`` in ``config/sources/aaii.yaml`` exist so the feed can be
   repointed at whichever provider is licensed without touching this
   module: any CSV whose first two columns are ``<iso-date>,<percent>``
   will parse. Until then this source logs the failure and yields zero
   rows rather than failing the ingest run.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.silver import SILVER_SCHEMA, QuestionType

if TYPE_CHECKING:  # pragma: no cover - keeps the pure logic importable w/o Spark
    from pyspark.sql import DataFrame, SparkSession

_log = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_DEFAULT_SERIES: Mapping[str, str] = {
    "bullish": "BULLISH",
    "bearish": "BEARISH",
    "neutral": "NEUTRAL",
}
_DEFAULT_MIN_DATE = "2020-01-01"

#: Outcome labels, and the series keys that supply their prices. The two
#: tuples are parallel and must stay that way — ``prices[i]`` is the share
#: for ``outcomes[i]``.
_OUTCOMES = ["Bullish", "Bearish", "Neutral"]
_SERIES_KEYS = ("bullish", "bearish", "neutral")

_USER_AGENT = "wisdom-of-crowds/0.1"
_TIMEOUT_SECONDS = 60
_MAX_ATTEMPTS = 3
_BACKOFF_SECONDS = 1.5

#: Statuses worth a second try. Everything else (404, 401, …) is a settled
#: answer from the server; retrying only slows the run down.
_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class AAIISource(Source):
    source_id = "aaii"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        base_url = cfg.params.get("fred_base_url", _DEFAULT_BASE_URL)
        series = dict(cfg.params.get("series") or _DEFAULT_SERIES)
        min_date = _parse_iso_date(cfg.params.get("min_date", _DEFAULT_MIN_DATE))

        observations: dict[str, dict[dt.date, float]] = {}
        for key in _SERIES_KEYS:
            series_id = series.get(key)
            if not series_id:
                _log.warning("aaii_series_not_configured", extra={"series_key": key})
                return self._empty(spark)

            raw = self._fetch_csv(base_url, series_id)
            if raw is None:
                # One missing leg makes every week incomplete, so there is
                # nothing partial worth emitting. Empty batch, no raise.
                return self._empty(spark)
            observations[key] = _parse_series_csv(raw)

        rows = list(_silver_rows(observations, min_date, cfg))
        if not rows:
            _log.warning("aaii_no_complete_weeks", extra={"min_date": min_date.isoformat()})
            return self._empty(spark)

        _log.info("aaii_extract_complete", extra={"rows": len(rows)})
        return spark.createDataFrame(rows, SILVER_SCHEMA)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty(spark: SparkSession) -> DataFrame:
        """A zero-row batch in the right shape — silver stays consistent."""
        return spark.createDataFrame([], SILVER_SCHEMA)

    @staticmethod
    def _fetch_csv(base_url: str, series_id: str) -> str | None:
        """Download one series' CSV, or return ``None`` if it cannot be had.

        Transient failures are retried with exponential backoff. Returning
        ``None`` rather than raising keeps one bad upstream day from failing
        the whole ingest run; the caller turns it into an empty batch.
        """
        url = f"{base_url}?{urllib.parse.urlencode({'id': series_id})}"
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            reason: str
            try:
                # Trusted host, URL built from config rather than user input.
                with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS) as resp:
                    # utf-8-sig: FRED prefixes a BOM, which would otherwise
                    # ride along on the first column of the header row.
                    return resp.read().decode("utf-8-sig")
            except urllib.error.HTTPError as exc:  # subclass of URLError; catch first
                if exc.code not in _RETRYABLE_STATUS:
                    _log.warning(
                        "aaii_download_failed",
                        extra={"series_id": series_id, "url": url, "status": exc.code},
                    )
                    return None
                reason = f"HTTP {exc.code}"
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                reason = type(exc).__name__

            _log.warning(
                "aaii_download_retry",
                extra={"series_id": series_id, "attempt": attempt, "reason": reason},
            )
            if attempt < _MAX_ATTEMPTS:
                time.sleep(_BACKOFF_SECONDS * 2 ** (attempt - 1))

        _log.warning(
            "aaii_download_gave_up",
            extra={"series_id": series_id, "url": url, "attempts": _MAX_ATTEMPTS},
        )
        return None


# ---------------------------------------------------------------------------
# pure helpers — no Spark, no network, so they unit-test directly
# ---------------------------------------------------------------------------


def _parse_iso_date(value: object) -> dt.date:
    """Coerce a config value to a date. YAML may already have parsed it."""
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value).strip())


def _parse_series_csv(raw: str) -> dict[dt.date, float]:
    """Parse a one-series CSV into ``{observation_date: value}``.

    Deliberately indifferent to column *names* — FRED heads the value column
    with the series ID and other providers use their own — so we read the
    first column as the date and the second as the value. Rows where that
    pair does not parse are skipped, which covers both the header line and
    the ``.`` FRED writes for a missing observation.
    """
    observations: dict[dt.date, float] = {}
    for row in csv.reader(io.StringIO(raw)):
        if len(row) < 2:
            continue
        try:
            day = dt.date.fromisoformat(row[0].strip())
            value = float(row[1].strip())
        except ValueError:
            continue
        observations[day] = value
    return observations


def _silver_rows(
    observations: Mapping[str, Mapping[dt.date, float]],
    min_date: dt.date,
    cfg: SourceConfig,
) -> Iterator[tuple]:
    """Yield one silver tuple per survey week present in *every* series."""
    for day in _complete_weeks(observations, min_date):
        prices = [float(observations[key][day]) / 100.0 for key in _SERIES_KEYS]
        iso = day.isoformat()
        yield (
            AAIISource.source_id,              # source
            f"aaii:{iso}",                     # market_id
            (                                  # question
                "AAII Investor Sentiment: bullish vs bearish vs neutral "
                f"(week of {iso})"
            ),
            QuestionType.POLL_CATEGORICAL,     # question_type
            None,                              # guesses — aggregate only
            list(_OUTCOMES),                   # outcomes
            prices,                            # prices
            None,                              # trader_count — n not published
            None,                              # volume_usd
            None,                              # end_date
            False,                             # is_resolved — a poll never resolves
            None,                              # resolved_outcome
            None,                              # resolved_value
            cfg.cycle_ts,                      # cycle_ts
        )


def _complete_weeks(
    observations: Mapping[str, Mapping[dt.date, float]],
    min_date: dt.date,
) -> Iterable[dt.date]:
    """Dates carried by all three series, from ``min_date`` on, oldest first."""
    shared: set[dt.date] | None = None
    for key in _SERIES_KEYS:
        days = set(observations.get(key, {}))
        shared = days if shared is None else (shared & days)
    return sorted(day for day in (shared or set()) if day >= min_date)
