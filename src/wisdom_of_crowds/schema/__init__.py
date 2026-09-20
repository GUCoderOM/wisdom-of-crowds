"""Shared Spark schemas for the pipeline's tables.

* :mod:`guesses`  — the normalised per-source input to the aggregation step.
* :mod:`wisdom`   — the human-facing wisdom-of-crowds output table.
* :mod:`bets`     — per-individual rows, written side by side by every
                    source that exposes individual-level activity.
* :mod:`sources`  — the source dimension table.
* :mod:`contract` — legacy guesses-shape assertion, kept for
                    ``transforms.py`` backward compatibility.
"""

from wisdom_of_crowds.schema import bets, contract, guesses, sources, wisdom  # noqa: F401
from wisdom_of_crowds.schema.contract import SchemaValidationError, assert_guesses_schema  # noqa: F401
