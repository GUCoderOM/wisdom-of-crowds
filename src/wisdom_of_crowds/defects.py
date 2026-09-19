"""The vocabulary of defect reasons.

Galton removed only cards that were *"defective or illegible"*
(*Vox Populi*, Nature 75:450, 1907). We do the same, but we say **why** each
row is defective, using a closed vocabulary defined here.

Every value is a plain string (via :class:`StrEnum`) so it can travel
through Spark columns, JSON logs, and downstream analytics unchanged.
"""

from __future__ import annotations

from enum import StrEnum


class DefectReason(StrEnum):
    """The reasons a guesses-row can be classified defective/illegible.

    Values are the strings that appear in the ``defect_reasons`` array
    column produced by :func:`wisdom_of_crowds.transforms.flag_defective`.
    """

    MISSING_NAME       = "missing_name"
    """The ``name`` column is null or an empty/whitespace-only string."""

    MISSING_GUESS      = "missing_guess"
    """The ``guess`` column is null. Non-integer text becomes null at load
    time because we load with an explicit :class:`IntegerType` schema, so
    unreadable numbers land in this bucket."""

    NON_POSITIVE_GUESS = "non_positive_guess"
    """The guess is zero or negative — not a legible answer to
    "how many sweets are in the jar"."""
