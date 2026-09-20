"""Strategy for categorical / poll questions.

Given N outcomes with N parallel prices/vote-shares, the crowd's answer is
the outcome with the highest probability, and ``wisdom`` is that value.
Used by multi-outcome prediction markets and POLL-style markets.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def aggregate() -> Column:
    return F.expr(
        """
        struct(
          array_max(prices)                                                          as wisdom,
          element_at(outcomes, CAST(array_position(prices, array_max(prices)) AS INT)) as wisdom_outcome,
          coalesce(trader_count, 0L)                                                 as guesses_count
        )
        """
    )
