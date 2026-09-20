"""Philadelphia Fed Survey of Professional Forecasters ingestion.

One row per (forecaster, variable, survey round, horizon). The
Philadelphia Fed publishes anonymised per-forecaster microdata as a
single Excel workbook — one sheet per macro variable, columns
``<var>1``..``<var>6`` for point forecasts at horizons 1-6 quarters
ahead. Each row in a sheet is one forecaster's answers for that survey
round.

We emit one ``answers`` row per (forecaster ``ID``, variable, survey
round, horizon), with:

* ``name``           — SPF forecaster ID (anonymised by the Fed)
* ``answer_value``   — that forecaster's point forecast
* ``answer_outcome`` — ``NULL`` (numeric)

Reference:
  https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/individual-forecasts
"""

from __future__ import annotations

import io
import logging
import urllib.request
from typing import Iterable

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.answers import INGEST_ANSWER_SCHEMA, QuestionType

_log = logging.getLogger(__name__)

_DEFAULT_URL = (
    "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/"
    "survey-of-professional-forecasters/historical-data/SPFmicrodata.xlsx"
)


class SPFSource(Source):
    slug = "spf"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        url         = cfg.params.get("url", _DEFAULT_URL)
        variables   = list(cfg.params.get("variables", ["UNEMP", "RGDP", "CPI"]))
        min_year    = int(cfg.params.get("min_year", 2015))
        max_horizon = int(cfg.params.get("max_horizon", 6))

        _log.info("spf_download_start", extra={"url": url})
        raw_bytes = self._download(url)

        rows = list(self._rows_from_xlsx(raw_bytes, variables, min_year, max_horizon, cfg))
        if not rows:
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    @staticmethod
    def _download(url: str) -> bytes:
        req = urllib.request.Request(url, headers={"User-Agent": "wisdom-of-crowds/0.4"})
        with urllib.request.urlopen(req, timeout=120) as r:  # noqa: S310 — trusted host
            return r.read()

    @staticmethod
    def _rows_from_xlsx(
        raw:         bytes,
        variables:   list[str],
        min_year:    int,
        max_horizon: int,
        cfg:         SourceConfig,
    ) -> Iterable[tuple]:
        book = pd.ExcelFile(io.BytesIO(raw))
        for var in variables:
            if var not in book.sheet_names:
                _log.warning("spf_variable_missing", extra={"variable": var})
                continue
            df = book.parse(var)
            df = df[df["YEAR"] >= min_year].copy()
            df["YEAR"]    = df["YEAR"].astype(int)
            df["QUARTER"] = df["QUARTER"].astype(int)
            # The forecaster identifier column is ``ID`` on every SPF
            # microdata sheet — anonymised by the Fed to a small integer.
            id_col = "ID" if "ID" in df.columns else None

            for horizon in range(1, max_horizon + 1):
                col = f"{var}{horizon}"
                if col not in df.columns:
                    continue

                sub = df.dropna(subset=[col])
                for _, r in sub.iterrows():
                    survey_y = int(r["YEAR"])
                    survey_q = int(r["QUARTER"])
                    target_y, target_q = _add_quarters(survey_y, survey_q, horizon - 1)

                    market_id = f"SPF_{var}_{survey_y}Q{survey_q}_H{horizon}"
                    question  = (
                        f"SPF: forecast of {var} for {target_y}Q{target_q} "
                        f"(survey {survey_y}Q{survey_q}, horizon {horizon}Q)"
                    )
                    name = str(int(r[id_col])) if id_col is not None else None

                    yield (
                        SPFSource.slug,                 # source
                        market_id,                      # market_id
                        question,                       # question
                        QuestionType.NUMERIC_GUESSES,   # question_type
                        name,                           # name (forecaster ID)
                        float(r[col]),                  # answer_value
                        None,                           # answer_outcome
                        None,                           # weight
                        None,                           # created_at
                        cfg.cycle_ts,                   # cycle_ts
                    )


def _add_quarters(year: int, quarter: int, delta: int) -> tuple[int, int]:
    zero_based = (year * 4 + (quarter - 1)) + delta
    return zero_based // 4, (zero_based % 4) + 1
