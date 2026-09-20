"""Sweets-jar ingestion — one question, N guessers.

Reads the ``name, guess`` CSV that started the project and produces exactly
one silver row: ``question_type = 'numeric_guesses'`` with the ``guesses``
array populated. The gift's original dataset now flows through the same
pipeline as any other source.
"""

from __future__ import annotations

import csv
import pathlib

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.silver import INGEST_SCHEMA, QuestionType


class SweetsJarSource(Source):
    """Read the tiny ``name,guess`` CSV bundled with this project.

    The CSV is small (~60 rows — the Galton replication), so we parse it in the driver with
    stdlib ``csv`` and hand the result to ``spark.createDataFrame``. That
    avoids two headaches:

    * ``spark.read.csv`` requires a filesystem Spark can see. On
      Databricks serverless, the ephemeral Python env
      (``/local_disk0/.ephemeral_nfs/…``) is not one of those, so a wheel
      resource read straight from disk fails with
      ``LocalFilesystemAccessDeniedException``.
    * The dataset is meant to work identically locally and on Databricks
      without asking the user to upload the CSV to a Volume first.
    """

    slug = "sweets_jar"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        input_path = self._resolve_input_path(cfg.params["input_path"])
        question   = cfg.params.get("question", "How many sweets are in the jar?")
        market_id  = cfg.params.get("market_id", "sweets-jar-original")
        resolved   = cfg.params.get("resolved_value")  # optional

        guesses = self._read_guesses(input_path)

        row = (
            self.slug,
            market_id,
            question,
            QuestionType.NUMERIC_GUESSES,
            guesses,              # guesses (list[float])
            None,                 # outcomes
            None,                 # prices
            None,                 # trader_count
            None,                 # volume_usd
            None,                 # end_date
            resolved is not None, # is_resolved
            None,                 # resolved_outcome
            float(resolved) if resolved is not None else None,  # resolved_value
            cfg.cycle_ts,         # cycle_ts
        )
        return spark.createDataFrame([row], INGEST_SCHEMA)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _read_guesses(path: str) -> list[float]:
        """Parse the ``name,guess`` CSV into a list of floats."""
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            return [float(row["guess"]) for row in reader if row.get("guess")]

    @staticmethod
    def _resolve_input_path(raw_path: str) -> str:
        """Resolve a possibly-relative ``input_path`` to an absolute
        filesystem path we can open with ``open()``.

        Searches, in order: the wheel's packaged ``_data/`` (force-included
        by ``pyproject.toml``), then the repo checkout's ``data/`` (for
        local dev). Absolute paths are returned untouched.
        """
        p = pathlib.Path(raw_path)
        if p.is_absolute():
            return str(p)

        here = pathlib.Path(__file__).resolve()
        for parent in [here.parent, *here.parents]:
            for prefix in ("_data", "data"):
                candidate = parent / prefix / p.name
                if candidate.is_file():
                    return str(candidate)
        # Fall through — return the raw path so the eventual FileNotFoundError
        # names the missing resource clearly.
        return raw_path
