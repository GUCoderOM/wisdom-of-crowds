"""Philadelphia Fed Survey of Professional Forecasters ingestion.

Downloads the single `SPFmicrodata.xlsx` file the Philadelphia Fed
maintains at [1]. That file has one sheet per macro variable
(``UNEMP``, ``RGDP``, ``CPI``, …). Each sheet has one row per
``(survey_year, survey_quarter, forecaster_id)`` with columns
``<var>1``…``<var>6`` for the point forecasts at horizons 1–6 quarters
ahead (index 1 = current quarter nowcast).

We turn each ``(variable, survey_year, survey_quarter, horizon)`` into one
silver row of ``question_type = 'numeric_guesses'``. The ``guesses`` array
contains every forecaster's answer for that question. The ``question`` text
is a plain-English rendering:

    "SPF: forecast of <VAR> for <TARGET_YEAR>-Q<TARGET_QUARTER>
     (survey <SURVEY_YEAR>-Q<SURVEY_QUARTER>, horizon <H>Q)"

References:
  [1] https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/individual-forecasts
"""

from __future__ import annotations

import io
import logging
import urllib.request
from typing import Iterable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.silver import SILVER_SCHEMA, QuestionType, SilverColumns

_log = logging.getLogger(__name__)

_DEFAULT_URL = (
    "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/"
    "survey-of-professional-forecasters/historical-data/SPFmicrodata.xlsx"
)


class SPFSource(Source):
    source_id = "spf"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        url         = cfg.params.get("url", _DEFAULT_URL)
        variables   = list(cfg.params.get("variables", ["UNEMP", "RGDP", "CPI"]))
        min_year    = int(cfg.params.get("min_year", 2015))
        max_horizon = int(cfg.params.get("max_horizon", 6))

        _log.info("spf_download_start", extra={"url": url})
        raw_bytes = self._download(url)

        rows = list(self._rows_from_xlsx(raw_bytes, variables, min_year, max_horizon, cfg))
        if not rows:
            # Empty batch is legal; caller sees zero-row DataFrame with the
            # correct schema, silver stays consistent.
            return spark.createDataFrame([], SILVER_SCHEMA)

        return spark.createDataFrame(rows, SILVER_SCHEMA)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _download(url: str) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "wisdom-of-crowds/0.1"})
        with urllib.request.urlopen(req, timeout=120) as r:  # noqa: S310 — trusted host
            return r.read()

    @staticmethod
    def _rows_from_xlsx(
        raw: bytes,
        variables: list[str],
        min_year: int,
        max_horizon: int,
        cfg: SourceConfig,
    ) -> Iterable[tuple]:
        book = pd.ExcelFile(io.BytesIO(raw))
        for var in variables:
            if var not in book.sheet_names:
                _log.warning("spf_variable_missing", extra={"variable": var})
                continue
            df = book.parse(var)
            df = df[df["YEAR"] >= min_year].copy()
            df["YEAR"] = df["YEAR"].astype(int)
            df["QUARTER"] = df["QUARTER"].astype(int)

            for horizon in range(1, max_horizon + 1):
                col = f"{var}{horizon}"
                if col not in df.columns:
                    continue

                # Group all forecasters for a given (survey year, quarter, horizon).
                grouped = (
                    df.dropna(subset=[col])
                      .groupby(["YEAR", "QUARTER"])[col]
                      .apply(lambda s: [float(x) for x in s.tolist()])
                      .reset_index()
                )
                for _, row in grouped.iterrows():
                    survey_y   = int(row["YEAR"])
                    survey_q   = int(row["QUARTER"])
                    guesses    = list(row[col])
                    target_y, target_q = _add_quarters(survey_y, survey_q, horizon - 1)

                    market_id = f"SPF_{var}_{survey_y}Q{survey_q}_H{horizon}"
                    question  = (
                        f"SPF: forecast of {var} for {target_y}Q{target_q} "
                        f"(survey {survey_y}Q{survey_q}, horizon {horizon}Q)"
                    )

                    yield (
                        SPFSource.source_id,   # source
                        market_id,             # market_id
                        question,              # question
                        QuestionType.NUMERIC_GUESSES,  # question_type
                        guesses,               # guesses
                        None,                  # outcomes
                        None,                  # prices
                        None,                  # trader_count
                        None,                  # volume_usd
                        None,                  # end_date
                        False,                 # is_resolved — SPF ground truth is joined later
                        None,                  # resolved_outcome
                        None,                  # resolved_value
                        cfg.cycle_ts,          # cycle_ts
                    )


def _add_quarters(year: int, quarter: int, delta: int) -> tuple[int, int]:
    """Return the ``(year, quarter)`` that is ``delta`` quarters after
    ``(year, quarter)``. Quarters are 1-indexed."""
    zero_based = (year * 4 + (quarter - 1)) + delta
    return zero_based // 4, (zero_based % 4) + 1
