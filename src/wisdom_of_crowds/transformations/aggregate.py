"""``aggregate_wisdom`` — group ``answers`` into ``wisdom``.

One input (``answers``), one output (``wisdom``). Framework applies the
``(cycle_dt, source_id)`` partition filter before we see the DataFrame,
so this transformation is a pure GROUP BY.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.aggregate import build_wisdom
from wisdom_of_crowds.framework import OutputSpec, WriteMode, register


class AggregateWisdomTransformation:
    """Read ``answers`` slice → write ``wisdom`` slice."""

    name         = "aggregate_wisdom"
    input_keys   = ("answers",)
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
        answers  = inputs["answers"]
        cycle_dt = _to_date(params["cycle_dt"])
        return {"wisdom": build_wisdom(answers, cycle_dt)}


register(AggregateWisdomTransformation())


def _to_date(v: Any) -> dt.date:
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v))
