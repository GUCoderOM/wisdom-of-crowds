"""Shared Spark schemas for the medallion layers.

* :mod:`silver`   — the normalised per-source input to the aggregation step.
* :mod:`gold`     — the human-facing wisdom-of-crowds output table.
* :mod:`contract` — legacy guesses-shape assertion, kept for
                    ``transforms.py`` backward compatibility.
"""

from wisdom_of_crowds.schema import contract, gold, silver  # noqa: F401
from wisdom_of_crowds.schema.contract import SchemaValidationError, assert_guesses_schema  # noqa: F401
