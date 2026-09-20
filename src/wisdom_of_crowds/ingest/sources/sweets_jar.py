"""Sweets-jar ingestion — one question, N guessers.

Reads the ``name, guess`` CSV that started the project and produces exactly
one silver row: ``question_type = 'numeric_guesses'`` with the ``guesses``
array populated. The gift's original dataset now flows through the same
pipeline as any other source.
"""

from __future__ import annotations

import pathlib

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.silver import INGEST_SCHEMA, QuestionType, SilverColumns


class SweetsJarSource(Source):
    slug = "sweets_jar"

    _CSV_SCHEMA = StructType([
        StructField("name",  StringType(),  nullable=False),
        StructField("guess", IntegerType(), nullable=False),
    ])

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        input_path = self._resolve_input_path(cfg.params["input_path"])
        question   = cfg.params.get("question", "How many sweets are in the jar?")
        market_id  = cfg.params.get("market_id", "sweets-jar-original")
        resolved   = cfg.params.get("resolved_value")  # optional

        raw = (
            spark.read.option("header", "true")
            .schema(self._CSV_SCHEMA)
            .csv(input_path)
        )

        guesses = raw.agg(F.collect_list(F.col("guess").cast("double")).alias("guesses"))

        return guesses.select(
            F.lit(self.slug).alias(SilverColumns.SOURCE),
            F.lit(market_id).alias(SilverColumns.MARKET_ID),
            F.lit(question).alias(SilverColumns.QUESTION),
            F.lit(QuestionType.NUMERIC_GUESSES).alias(SilverColumns.QUESTION_TYPE),
            F.col("guesses").alias(SilverColumns.GUESSES),
            F.lit(None).cast(INGEST_SCHEMA[SilverColumns.OUTCOMES].dataType).alias(SilverColumns.OUTCOMES),
            F.lit(None).cast(INGEST_SCHEMA[SilverColumns.PRICES].dataType).alias(SilverColumns.PRICES),
            F.lit(None).cast("long").alias(SilverColumns.TRADER_COUNT),
            F.lit(None).cast("double").alias(SilverColumns.VOLUME_USD),
            F.lit(None).cast("timestamp").alias(SilverColumns.END_DATE),
            F.lit(resolved is not None).alias(SilverColumns.IS_RESOLVED),
            F.lit(None).cast("string").alias(SilverColumns.RESOLVED_OUTCOME),
            F.lit(resolved).cast("double").alias(SilverColumns.RESOLVED_VALUE),
            F.lit(cfg.cycle_ts).cast("timestamp").alias(SilverColumns.CYCLE_TS),
        )

    @staticmethod
    def _resolve_input_path(raw_path: str) -> str:
        """Resolve a possibly-relative ``input_path`` against the packaged
        wheel data (``_data/`` sibling of ``_config/``, force-included by
        ``pyproject.toml``) or the repo checkout, whichever is first.

        Databricks Spark rejects relative paths outright, so this
        deterministically upgrades e.g. ``data/sweets-jar-guesses.csv``
        into an absolute filesystem path. Absolute paths are returned
        untouched.
        """
        p = pathlib.Path(raw_path)
        if p.is_absolute():
            return str(p)

        here = pathlib.Path(__file__).resolve()
        for parent in [here.parent, *here.parents]:
            for prefix in ("data", "_data"):
                candidate = parent / prefix / p.name
                if candidate.is_file():
                    return f"file://{candidate}"
        # Last resort — return as-is so the caller sees the original
        # error rather than a silently-mangled path.
        return raw_path
