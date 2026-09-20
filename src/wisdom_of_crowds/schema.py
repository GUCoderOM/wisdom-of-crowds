"""Deprecated shim — kept only for import compatibility.

The actual schema code moved to :mod:`wisdom_of_crowds.schema` (a package).
Python resolves the package before this module, so this file is unreachable
at import time. It exists purely to make the git history remove-able cleanly.
"""

from wisdom_of_crowds.schema.contract import SchemaValidationError, assert_guesses_schema  # noqa: F401
