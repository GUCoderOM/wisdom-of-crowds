"""Parameter resolution for the notebook.

The notebook is designed to run identically in two places:

- Locally, in Jupyter, where parameters are hard-coded at the top of the
  notebook (as module-level constants).
- On Databricks, where parameters are supplied by notebook widgets.

The resolution rule, chosen deliberately so the same notebook file works in
both places: **the hard-coded config value wins if it is not None. Otherwise
we fall back to the Databricks widget of the same name.** If neither is
available, we raise a clear error.

Widgets only exist inside a Databricks notebook runtime. Attempting to import
`dbutils` outside Databricks raises, so we catch and treat that as "no widget
available", which is the correct behaviour for the local case.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class ParameterNotProvidedError(RuntimeError):
    """Raised when a parameter is missing from both the config and any widget."""


@dataclass(frozen=True)
class ExperimentConfig:
    """All parameters the notebook needs, in one place.

    Attributes:
        input_path:   Path to the guesses CSV. Local filesystem path when
                      running in Jupyter, a Unity Catalog Volume path
                      (``/Volumes/<catalog>/<schema>/<volume>/...``) when
                      running on Databricks.
        true_count:   The known number of sweets in the jar. This is Galton's
                      ground truth; the whole experiment is meaningless without
                      it. See Vox Populi (Nature 75:450): "The dressed weight
                      proved to be 1198 lbs."
        output_table: Where to write the summary. A Delta table name
                      (``catalog.schema.table``) on Databricks, or a local
                      directory path (Parquet) when running in Jupyter.
    """

    input_path: str
    true_count: int
    output_table: str


def _try_get_widget(name: str) -> str | None:
    """Return the value of a Databricks widget, or None if we're not on Databricks.

    Databricks injects a ``dbutils`` global into notebook execution. It is not
    importable as a normal Python module, so we probe the calling frame's
    globals via ``builtins`` first, then fall through cleanly.
    """
    try:
        # On Databricks, `dbutils` is a builtin-ish global available at runtime.
        # We look for it without importing anything that could crash locally.
        dbutils = globals().get("dbutils") or __import__("builtins").__dict__.get("dbutils")
        if dbutils is None:
            return None
        value = dbutils.widgets.get(name)  # type: ignore[attr-defined]
        # Widgets return "" when unset; treat empty as absent.
        return value if value != "" else None
    except Exception:
        return None


def _coerce(name: str, value: Any, cast: type) -> Any:
    """Cast a widget string to the target type, with a helpful error message."""
    try:
        return cast(value)
    except (TypeError, ValueError) as exc:
        raise ParameterNotProvidedError(
            f"Parameter {name!r} could not be interpreted as {cast.__name__}: {value!r}"
        ) from exc


def resolve(
    name: str,
    config_value: Any,
    cast: type = str,
) -> Any:
    """Return the parameter's value, preferring the config over the widget.

    Args:
        name:         Widget name to fall back to on Databricks.
        config_value: Value hard-coded in the notebook. If not None, wins.
        cast:         Target type for the widget's string value.

    Raises:
        ParameterNotProvidedError: If neither source supplies a value.
    """
    if config_value is not None:
        return config_value

    widget_value = _try_get_widget(name)
    if widget_value is None:
        raise ParameterNotProvidedError(
            f"Parameter {name!r} was not set in the notebook config and no "
            f"Databricks widget with that name was found. On Databricks, add "
            f"`dbutils.widgets.text({name!r}, ...)` and set a value. Locally, "
            f"set the constant at the top of the notebook."
        )
    return _coerce(name, widget_value, cast)


def build_config(
    input_path_cfg: str | None,
    true_count_cfg: int | None,
    output_table_cfg: str | None,
) -> ExperimentConfig:
    """Resolve every parameter into a single, validated config object."""
    return ExperimentConfig(
        input_path=resolve("input_path", input_path_cfg, cast=str),
        true_count=resolve("true_count", true_count_cfg, cast=int),
        output_table=resolve("output_table", output_table_cfg, cast=str),
    )
