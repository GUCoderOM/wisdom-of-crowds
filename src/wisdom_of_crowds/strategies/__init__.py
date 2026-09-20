"""Aggregation strategies — one per :class:`QuestionType`.

The aggregate job routes each silver row to the strategy whose
``question_type`` matches. Adding a new question type: add a strategy
module here, register it in :data:`STRATEGY_REGISTRY`.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import Column

from wisdom_of_crowds.schema.silver import QuestionType
from wisdom_of_crowds.strategies import binary_market, categorical, numeric_guesses

# A strategy is a pure function: given the columns of a silver row, return
# a Struct column with (wisdom: double, wisdom_outcome: string, guesses: long).
# Concrete strategies live in the submodules imported above.
Strategy = Callable[..., Column]


STRATEGY_REGISTRY: dict[str, Strategy] = {
    QuestionType.NUMERIC_GUESSES:  numeric_guesses.aggregate,
    QuestionType.BINARY:           binary_market.aggregate,
    QuestionType.CATEGORICAL:      categorical.aggregate,
    QuestionType.POLL_CATEGORICAL: categorical.aggregate,  # same shape
}
"""Question-type → aggregation function. The aggregate job dispatches on
this map; unknown ``question_type`` values fail loudly at build time."""
