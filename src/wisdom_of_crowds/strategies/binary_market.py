"""Strategy for binary prediction-market questions.

Given a market with two outcomes (typically ``["Yes","No"]``) and current
prices summing to ~1, the crowd's answer is the YES probability. We keep
``wisdom_outcome`` explicit so downstream analytics never has to think
about ordering.

Simple by design — we assume Polymarket-style ``"Yes"`` labelling and let
the caller normalise. The full case-insensitive lookup lives in a SQL
expression so we don't hit PySpark's Python-vs-Column-indexing edges.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def aggregate() -> Column:
    return F.expr(
        """
        struct(
          element_at(prices,   CAST(array_position(outcomes, 'Yes') AS INT)) as wisdom,
          element_at(outcomes, CAST(array_position(outcomes, 'Yes') AS INT)) as wisdom_outcome,
          coalesce(trader_count, 0L)                                          as guesses_count
        )
        """
    )
