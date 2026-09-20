"""Sweets-jar ingestion — one question, N guessers.

Reads the ``name, guess`` CSV that started the project and produces exactly
one silver row: ``question_type = 'numeric_guesses'`` with the ``guesses``
array populated. The gift's original dataset now flows through the same
pipeline as any other source.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.silver import SILVER_SCHEMA, QuestionType, SilverColumns


class SweetsJarSource(Source):
    source_id = "sweets_jar"

    _CSV_SCHEMA = StructType([
        StructField("name",  StringType(),  nullable=False),
        StructField("guess", IntegerType(), nullable=False),
    ])

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        input_path = cfg.params["input_path"]
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
            F.lit(self.source_id).alias(SilverColumns.SOURCE),
            F.lit(market_id).alias(SilverColumns.MARKET_ID),
            F.lit(question).alias(SilverColumns.QUESTION),
            F.lit(QuestionType.NUMERIC_GUESSES).alias(SilverColumns.QUESTION_TYPE),
            F.col("guesses").alias(SilverColumns.GUESSES),
            F.lit(None).cast(SILVER_SCHEMA[SilverColumns.OUTCOMES].dataType).alias(SilverColumns.OUTCOMES),
            F.lit(None).cast(SILVER_SCHEMA[SilverColumns.PRICES].dataType).alias(SilverColumns.PRICES),
            F.lit(None).cast("long").alias(SilverColumns.TRADER_COUNT),
            F.lit(None).cast("double").alias(SilverColumns.VOLUME_USD),
            F.lit(None).cast("timestamp").alias(SilverColumns.END_DATE),
            F.lit(resolved is not None).alias(SilverColumns.IS_RESOLVED),
            F.lit(None).cast("string").alias(SilverColumns.RESOLVED_OUTCOME),
            F.lit(resolved).cast("double").alias(SilverColumns.RESOLVED_VALUE),
            F.lit(cfg.cycle_ts).cast("timestamp").alias(SilverColumns.CYCLE_TS),
        )
