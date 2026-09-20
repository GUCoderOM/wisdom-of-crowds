"""Schema contract enforcement for the legacy sweets-jar entry point.

A cheap upfront guard so that a mis-shaped input DataFrame fails loudly at
the boundary. Kept for backward compatibility with the original
``transforms.py`` API; new code should use :mod:`wisdom_of_crowds.schema.silver`
directly.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType, IntegerType, StringType

from wisdom_of_crowds.columns import InputColumns


class SchemaValidationError(RuntimeError):
    """Raised when an input DataFrame violates its contract."""


_REQUIRED_GUESSES_COLUMNS: dict[str, DataType] = {
    InputColumns.NAME:  StringType(),
    InputColumns.GUESS: IntegerType(),
}


def assert_guesses_schema(df: DataFrame) -> None:
    """Assert *df* carries the required guesses columns with the right types."""
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
