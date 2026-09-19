"""Column-name constants.

Every column referenced by the pipeline appears here as a ``Final[str]``.
This turns renames into compile-time work (grep + rename), typos into import
errors, and code review into a matter of following symbols rather than
scanning for magic strings.

Two namespaces:

* :class:`InputColumns`  — columns present on the raw input DataFrame.
* :class:`FlagColumns`   — columns added by the flagging step.

Downstream code should always reference these constants, never string
literals, so the compiler is on our side.
"""

from __future__ import annotations

from typing import Final


class InputColumns:
    """Columns expected on the input DataFrame at load time."""

    NAME:  Final[str] = "name"
    GUESS: Final[str] = "guess"


class FlagColumns:
    """Columns added by :func:`wisdom_of_crowds.transforms.flag_defective`."""

    DEFECT_REASONS: Final[str] = "defect_reasons"
    IS_DEFECTIVE:   Final[str] = "is_defective"
