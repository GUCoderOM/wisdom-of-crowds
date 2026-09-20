"""Sweets-jar ingestion — one question, N guessers.

Reads the ``name, guess`` CSV that started the project and produces one
row per guess into the shared ``answers`` table. Each row carries the
guesser's ``name`` and their numeric ``answer_value``.
"""

from __future__ import annotations

import csv
import pathlib

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.answers import INGEST_ANSWER_SCHEMA, QuestionType


class SweetsJarSource(Source):
    slug = "sweets_jar"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        input_path = self._resolve_input_path(cfg.params["input_path"])
        question   = cfg.params.get("question", "How many sweets are in the jar?")
        market_id  = cfg.params.get("market_id", "sweets-jar-original")

        rows: list[tuple] = []
        with open(input_path, newline="") as f:
            for record in csv.DictReader(f):
                name  = record.get("name") or None
                raw_g = record.get("guess")
                if raw_g is None or raw_g == "":
                    continue
                rows.append((
                    self.slug,
                    market_id,
                    question,
                    QuestionType.NUMERIC_GUESSES,
                    name,
                    float(raw_g),         # answer_value
                    None,                 # answer_outcome
                    None,                 # weight
                    None,                 # created_at
                    cfg.cycle_ts,
                ))
        if not rows:
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    @staticmethod
    def _resolve_input_path(raw_path: str) -> str:
        p = pathlib.Path(raw_path)
        if p.is_absolute():
            return str(p)
        here = pathlib.Path(__file__).resolve()
        for parent in [here.parent, *here.parents]:
            for prefix in ("_data", "data"):
                candidate = parent / prefix / p.name
                if candidate.is_file():
                    return str(candidate)
        return raw_path
