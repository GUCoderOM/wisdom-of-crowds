"""Strategy for questions of shape ``numeric_guesses`` — Galton's original.

Given a row where the ``guesses`` array contains N individual numeric
guesses, the crowd's answer is the median. This is the sweets-jar
transform lifted to Spark SQL so it can run column-wise over any silver
row without collecting the array to the driver.

Galton, *One Vote, One Value* (Nature 75:414, 1907):

    "the estimate to which least objection can be raised is the
     middlemost estimate, the number of votes that it is too high
     being exactly balanced by the number of votes that it is too low."

Even-count rule (Galton's own): "the mean of the ``n``-th and
``(n+1)``-th" — implemented literally below.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def aggregate() -> Column:
    # Written as a SQL expression to avoid the Python-vs-Column indexing
    # edges of pyspark.sql.functions.element_at with computed positions.
    return F.expr(
        """
        struct(
          (
            element_at(array_sort(guesses), CAST(floor((size(guesses) - 1) / 2) AS INT) + 1)
            +
            element_at(array_sort(guesses), CAST(floor(size(guesses) / 2)       AS INT) + 1)
          ) / 2.0                                                       as wisdom,
          CAST(NULL AS STRING)                                          as wisdom_outcome,
          CAST(size(guesses) AS BIGINT)                                 as guesses_count
        )
        """
    )
