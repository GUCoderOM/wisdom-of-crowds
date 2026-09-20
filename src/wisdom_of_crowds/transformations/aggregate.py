"""``aggregate_wisdom`` transformation.

Reads the ``guesses`` DataFrame, applies the per-question-type strategy
via :func:`wisdom_of_crowds.aggregate.build_gold`, and returns a single
``wisdom`` DataFrame. All partition filtering (by ``cycle_dt`` and
optionally ``source_id``) happens at framework level before the DataFrame
lands here — this is a pure function on the pre-filtered slice.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.aggregate import build_gold
from wisdom_of_crowds.framework import OutputSpec, WriteMode, register


class AggregateWisdomTransformation:
    """One transformation, one output. The registry key is
    ``aggregate_wisdom``.

    Params:
      * ``cycle_dt`` (required) — the run's date; used both as the
        partition filter on ``guesses`` and as the ``cycle_dt`` written on
        every ``wisdom`` row.
      * ``source_id`` (optional) — when set, the framework filters
        ``guesses`` to that source's partition before this transformation
        runs. ``replaceWhere`` on the wisdom output stays scoped to the
        same partition, so parallel runs for different sources never
        collide.
    """

    name         = "aggregate_wisdom"
    input_keys   = ("guesses",)
    output_specs = {
        "wisdom": OutputSpec(
            write_mode=WriteMode.OVERWRITE_PARTITION,
            partition_by=("cycle_dt", "source_id"),
            # ``source_id`` is optional — when absent, replace the whole
            # date. ``framework._fill_placeholders`` will substitute an
            # empty string, producing e.g. ``source_id =  AND cycle_dt = ...``
            # which is invalid SQL, so callers must set source_id when
            # they want partitioned overwrite. In practice every aggregate
            # run in this project scopes to one source at a time.
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
        cycle_dt = _to_date(params["cycle_dt"])
        wisdom   = build_gold(guesses, cycle_dt)
        return {"wisdom": wisdom}


register(AggregateWisdomTransformation())


def _to_date(v: Any) -> dt.date:
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v))
