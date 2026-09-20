"""``aggregate_wisdom`` transformation.

Reads the ``guesses`` DataFrame, optionally reads the ``bets`` DataFrame
(the shared per-individual table), and returns a single ``wisdom``
DataFrame.

The ``bets`` input is optional: sources that don't have per-individual
data (SPF, sweets_jar, ECB, NOAA) rely on the ``guesses`` array column
alone. Sources that do (Manifold, Polymarket) supply ``bets`` so the
wisdom row's ``guesses`` count reflects the actual number of individual
trades — regardless of whether the trades carry user IDs. Anonymous
per-trade data still counts; each row is one datapoint.

Partition filtering (by ``cycle_dt`` and optionally ``source_id``)
happens at the framework level before the DataFrames arrive here.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wisdom_of_crowds.aggregate import build_gold
from wisdom_of_crowds.framework import OutputSpec, WriteMode, register
from wisdom_of_crowds.schema.wisdom import WisdomColumns


class AggregateWisdomTransformation:
    """Compute wisdom rows from a slice of guesses (optionally joined to
    the bets table for a true per-market data-point count).
    """

    name         = "aggregate_wisdom"
    #: ``guesses`` is required. ``bets`` is optional — the framework will
    #: pass it in ``inputs`` only when the payload names it. When absent,
    #: the count in wisdom falls back to ``size(guesses)`` (numeric
    #: sources) or the source's declared ``trader_count`` (markets).
    input_keys   = ("guesses",)
    optional_input_keys = ("bets",)
    output_specs = {
        "wisdom": OutputSpec(
            write_mode=WriteMode.OVERWRITE_PARTITION,
            partition_by=("cycle_dt", "source_id"),
            replace_where=(
                "source_id = {source_id} AND cycle_dt = date'{cycle_dt}'"
            ),
        ),
    }

    def run(
        self,
        spark:  SparkSession,
        inputs: Mapping[str, DataFrame],
        params: Mapping[str, Any],
    ) -> Mapping[str, DataFrame]:
        guesses  = inputs["guesses"]
        bets     = inputs.get("bets")
        cycle_dt = _to_date(params["cycle_dt"])

        wisdom = build_gold(guesses, cycle_dt)

        if bets is not None:
            # Count individual data points per (source_id, market_id) from
            # bets — every row is one participant's revealed answer.
            # A LEFT JOIN preserves markets with no bets in the current
            # partition; those keep whatever ``guesses`` count they had.
            per_market_count = (
                bets.groupBy("source_id", "market_id")
                    .agg(F.count(F.lit(1)).alias("_bet_count"))
            )
            wisdom = (
                wisdom.alias("w")
                      .join(per_market_count.alias("b"),
                            on=["source_id", "market_id"],
                            how="left")
                      .withColumn(
                          WisdomColumns.GUESSES,
                          F.coalesce(
                              F.col("_bet_count").cast("long"),
                              F.col(WisdomColumns.GUESSES),
                          ),
                      )
                      .drop("_bet_count")
                      # Preserve the original column order after the join.
                      .select(*[c for c in wisdom.columns])
            )

        return {"wisdom": wisdom}


register(AggregateWisdomTransformation())


def _to_date(v: Any) -> dt.date:
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v))
