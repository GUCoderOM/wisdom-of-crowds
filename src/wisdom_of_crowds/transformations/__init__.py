"""Transformation registry.

Importing this package registers every transformation with
:mod:`wisdom_of_crowds.framework`. Callers dispatch by name:

.. code-block:: python

    import wisdom_of_crowds.transformations  # noqa: F401  — registers everything
    from wisdom_of_crowds.framework import run_payload, TransformationPayload

    run_payload(spark, TransformationPayload.from_dict(payload_json))
"""

# Register every transformation by importing its module. Order does not
# matter — the registry checks for duplicate names.
from wisdom_of_crowds.transformations import ingest, aggregate  # noqa: F401
