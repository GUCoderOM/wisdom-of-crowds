"""Strategy for binary prediction-market questions.

A binary market has two outcomes (Yes/No, YES/NO, True/False — labels vary
by source) and prices summing to ~1. The crowd's answer is whichever
outcome carries the higher price. We take the max price as ``wisdom`` and
the corresponding outcome label as ``wisdom_outcome`` — this is
identical to the categorical case, just constrained to two outcomes, so
we work off ``array_max`` / ``array_position`` on prices rather than
hard-coding the label ``'Yes'`` (Polymarket uses ``'YES'``, others may
differ).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def aggregate() -> Column:
    return F.expr(
        """
        struct(
          array_max(prices)                                                    as wisdom,
          element_at(outcomes, CAST(array_position(prices, array_max(prices)) AS INT)) as wisdom_outcome,
          coalesce(trader_count, 0L)                                            as guesses_count
        )
        """,
    )
