"""Deprecated. The framework runner replaces the ``vox-ingest`` CLI.

To ingest a source, call :func:`wisdom_of_crowds.framework.run_payload`
with a ``TransformationPayload`` naming ``ingest_<slug>``, or trigger
the Databricks job with the same JSON payload.

Kept as an empty module so no direct import raises during transitional
deploys.
"""

from __future__ import annotations
