"""The generic aggregator — reads silver, applies strategies, writes gold.

Runs identically locally (against a Parquet silver table) and on Databricks
(against a Delta silver table). The only difference is the ``spark`` session
and the path/table used for I/O.

Gold is partitioned by ``(cycle_dt, source_id)`` — the same partition
scheme as silver — so parallel ingest+aggregate runs of different sources
never touch each other's gold partitions.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wisdom_of_crowds.schema.gold import GoldColumns
from wisdom_of_crowds.schema.silver import SilverColumns
from wisdom_of_crowds.strategies import STRATEGY_REGISTRY

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AggregateJobConfig:
    """Runtime parameters for :func:`run`. Passed as CLI args or notebook
    widgets, resolved via the same convention we use elsewhere."""

    silver_source:  str          # a table name (``cat.sch.tab``) or a path
    gold_output:    str          # a Delta table name or a Parquet directory
    cycle_dt:       dt.date      # partition key for this run
    source_id:      int | None = None  # if set, aggregate only this source's slice


def run(spark: SparkSession, cfg: AggregateJobConfig) -> DataFrame:
    """Read silver, apply the correct strategy per row, write gold, return
    the gold DataFrame for verification."""
    silver = _read(spark, cfg.silver_source, cycle_dt=cfg.cycle_dt, source_id=cfg.source_id)
    gold = build_gold(silver, cfg.cycle_dt)
    _write(gold, cfg.gold_output, cycle_dt=cfg.cycle_dt, source_id=cfg.source_id)
    _log.info("aggregate_complete", extra={
        "rows_written": gold.count(),
        "cycle_dt": cfg.cycle_dt.isoformat(),
        "source_id": cfg.source_id,
        "gold_output": cfg.gold_output,
    })
    return gold


def build_gold(silver: DataFrame, cycle_dt: dt.date) -> DataFrame:
    """Compute gold rows from silver — pure function, no I/O.

    Dispatches on ``question_type``: each strategy contributes a struct
    ``(wisdom, wisdom_outcome, guesses_count)``; we then compute the error
    columns and assemble the final gold shape (which now carries
    ``source_id`` alongside the ``source`` slug).
    """
    when_chain = None
    for qtype, strategy in STRATEGY_REGISTRY.items():
        expr = strategy()
        when_chain = (
            F.when(F.col(SilverColumns.QUESTION_TYPE) == qtype, expr)
            if when_chain is None
            else when_chain.when(F.col(SilverColumns.QUESTION_TYPE) == qtype, expr)
        )
    result = when_chain
    assert result is not None, "STRATEGY_REGISTRY is empty"

    projected = silver.withColumn("_result", result)

    gold = projected.select(
        F.col(SilverColumns.SOURCE_ID).alias(GoldColumns.SOURCE_ID),
        F.col(SilverColumns.SOURCE).alias(GoldColumns.SOURCE),
        F.col(SilverColumns.MARKET_ID).alias(GoldColumns.MARKET_ID),
        F.col(SilverColumns.QUESTION).alias(GoldColumns.QUESTION),
        F.col(SilverColumns.QUESTION_TYPE).alias(GoldColumns.QUESTION_TYPE),
        F.col("_result.wisdom").alias(GoldColumns.WISDOM),
        F.col("_result.wisdom_outcome").alias(GoldColumns.WISDOM_OUTCOME),
        F.col("_result.guesses_count").alias(GoldColumns.GUESSES),
        F.col(SilverColumns.RESOLVED_VALUE).alias(GoldColumns.RESOLVED_VALUE),
        (F.col("_result.wisdom") - F.col(SilverColumns.RESOLVED_VALUE))
            .alias(GoldColumns.SIGNED_ERROR),
        F.when(
            F.col(SilverColumns.RESOLVED_VALUE).isNotNull() &
            (F.col(SilverColumns.RESOLVED_VALUE) != 0),
            100.0 * (F.col("_result.wisdom") - F.col(SilverColumns.RESOLVED_VALUE))
                  / F.col(SilverColumns.RESOLVED_VALUE),
        ).alias(GoldColumns.SIGNED_PERCENT_ERROR),
        F.lit(cycle_dt).cast("date").alias(GoldColumns.CYCLE_DT),
    )
    # Contract: gold and silver stay 1:1 per (source_id, market_id,
    # cycle_dt). Rows where the strategy couldn't compute a wisdom
    # (e.g. a categorical market with an empty answers array) keep a
    # NULL wisdom in gold — that null is a queryable data-quality
    # signal, not a hidden failure. Downstream analytics filter
    # ``wisdom IS NOT NULL`` when they want only the answered rows.
    return gold


# ---------------------------------------------------------------------------
# I/O helpers — table vs path, Delta vs Parquet, resolved from a single string
# ---------------------------------------------------------------------------


def _is_table_name(target: str) -> bool:
    return ("/" not in target) and (target.count(".") >= 1)


def _read(
    spark: SparkSession,
    source: str,
    *,
    cycle_dt: dt.date,
    source_id: int | None,
) -> DataFrame:
    """Read silver, scoped down to the partition(s) being aggregated so a
    single-source aggregate doesn't scan the entire history."""
    df = spark.read.table(source) if _is_table_name(source) else spark.read.parquet(source)
    df = df.filter(F.col(SilverColumns.CYCLE_DT) == F.lit(cycle_dt).cast("date"))
    if source_id is not None:
        df = df.filter(F.col(SilverColumns.SOURCE_ID) == source_id)
    return df


def _write(
    df: DataFrame,
    target: str,
    *,
    cycle_dt: dt.date,
    source_id: int | None,
) -> None:
    """Write gold with the same ``(cycle_dt, source_id)`` partition scheme
    as silver. When ``source_id`` is provided, ``replaceWhere`` scopes the
    overwrite so parallel aggregates for different sources on the same day
    don't clobber each other."""
    if source_id is not None:
        replace_where = (
            f"cycle_dt = date'{cycle_dt.isoformat()}' AND source_id = {source_id}"
        )
    else:
        replace_where = f"cycle_dt = date'{cycle_dt.isoformat()}'"

    if _is_table_name(target):
        (
            df.write.format("delta").mode("overwrite")
              .option("replaceWhere", replace_where)
              .partitionBy(GoldColumns.CYCLE_DT, GoldColumns.SOURCE_ID)
              .saveAsTable(target)
        )
    else:
        (
            df.write.mode("overwrite")
              .partitionBy(GoldColumns.CYCLE_DT, GoldColumns.SOURCE_ID)
              .parquet(target)
        )
