"""Schema contract enforcement.

A cheap upfront guard so that a mis-shaped input DataFrame fails loudly at
the boundary, not silently three transformations later. Fail fast, fail
close to the cause.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType, IntegerType, StringType

from wisdom_of_crowds.columns import InputColumns


class SchemaValidationError(RuntimeError):
    """Raised when an input DataFrame violates its contract."""


# The single source of truth for what the guesses DataFrame must look like
# before any transformation runs on it. Mirrors ``GUESSES_SCHEMA`` in
# ``transforms.py``, but expressed as a name→type map so we can check
# individual columns rather than requiring an exact schema match (extra
# columns are fine; missing or wrong-typed columns are not).
_REQUIRED_GUESSES_COLUMNS: dict[str, DataType] = {
    InputColumns.NAME:  StringType(),
    InputColumns.GUESS: IntegerType(),
}


def assert_guesses_schema(df: DataFrame) -> None:
    """Assert *df* carries the required guesses columns with the right types.

    Extra columns are permitted (this is what makes the function safe to
    call after :func:`flag_defective`, which adds columns of its own).
    Missing or wrong-typed columns raise :class:`SchemaValidationError`
    with a message that names every offender.

    Args:
        df: The DataFrame to check.

    Raises:
        SchemaValidationError: If any required column is missing or has the
            wrong :class:`DataType`.
    """
    actual = {field.name: field.dataType for field in df.schema.fields}

    problems: list[str] = []
    for name, expected_type in _REQUIRED_GUESSES_COLUMNS.items():
        if name not in actual:
            problems.append(f"missing column {name!r}")
        elif actual[name] != expected_type:
            problems.append(
                f"column {name!r} has type {actual[name].simpleString()!r}, "
                f"expected {expected_type.simpleString()!r}"
            )

    if problems:
        raise SchemaValidationError(
            "Input DataFrame does not match the guesses contract: "
            + "; ".join(problems)
        )
