"""Transformation framework — one payload shape, N named transformations.

Every job run in the pipeline is one call to :func:`run_payload` with a
payload of the shape:

.. code-block:: json

    {
      "transformation_name": "ingest_polymarket",
      "inputs":  { "guesses": "wisdom_of_crowds.core.guesses" },
      "outputs": { "wisdom":  "wisdom_of_crowds.core.wisdom" },
      "params":  { "cycle_dt": "2026-09-20", "source_slug": "polymarket" }
    }

The framework reads every ``inputs[*]`` into a DataFrame (applying any
partition-filter params whose column exists on the frame), hands the
resulting ``dict[str, DataFrame]`` to the transformation's :func:`run`
method, and writes each returned DataFrame back to the matching
``outputs[*]`` target using the transformation's per-output
:class:`OutputSpec` (partition scheme + write mode).

Transformations register themselves in
:mod:`wisdom_of_crowds.transformations`.
"""

from wisdom_of_crowds.framework.transformation import (  # noqa: F401
    OutputSpec,
    Transformation,
    TransformationPayload,
    WriteMode,
    get_transformation,
    register,
    registered_names,
    run_payload,
)
