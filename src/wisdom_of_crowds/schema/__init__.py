"""Shared Spark schemas for the pipeline's tables.

* :mod:`answers`  — one row per individual guess/bet; the single
                    per-individual table every source writes into.
* :mod:`wisdom`   — the human-facing wisdom-of-crowds output table,
                    aggregated from :mod:`answers`.
* :mod:`sources`  — the source dimension table.
* :mod:`contract` — legacy guesses-shape assertion, kept for
                    ``transforms.py`` backward compatibility.
"""

from wisdom_of_crowds.schema import answers, contract, sources, wisdom  # noqa: F401
from wisdom_of_crowds.schema.contract import SchemaValidationError, assert_guesses_schema  # noqa: F401
