# Databricks notebook source
# Single parameterised task for the ``wisdom_of_crowds_pipeline`` job.
#
# Everest / DAB convention: one job, one task, one payload. The whole
# run is identified by ``transformation_name`` inside the payload.
#
# Parameters:
#   transformation_payload - a JSON string of shape:
#     {
#       "transformation_name": "ingest_polymarket",
#       "inputs":  { ... table refs by logical key ... },
#       "outputs": { ... table refs by logical key ... },
#       "params":  { "cycle_dt": "2026-09-20", ... }
#     }
#   wheel_path - workspace path to the installed wheel.
#
# The notebook installs the wheel, restarts Python, and hands the payload
# to the framework runner which dispatches to a registered transformation.

dbutils.widgets.text(
    "transformation_payload",
    '{"transformation_name": "ingest_spf", '
    '"inputs": {}, '
    '"outputs": {'
    '"guesses": "wisdom_of_crowds.core.guesses", '
    '"sources": "wisdom_of_crowds.core.sources"'
    '}, '
    '"params": {}}',
)
dbutils.widgets.text("wheel_path", "")

# COMMAND ----------

import subprocess
import sys

_proc = subprocess.run(
    [sys.executable, "-m", "pip", "install", dbutils.widgets.get("wheel_path")],
    capture_output=True,
    text=True,
)
print(_proc.stdout)
if _proc.returncode != 0:
    raise RuntimeError(
        "pip install failed (exit %d)\n--- stdout ---\n%s\n--- stderr ---\n%s"
        % (_proc.returncode, _proc.stdout, _proc.stderr)
    )

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# Ensure catalog + schema exist (idempotent).
spark.sql("CREATE CATALOG IF NOT EXISTS wisdom_of_crowds")
spark.sql("CREATE SCHEMA IF NOT EXISTS wisdom_of_crowds.core")

payload_str = dbutils.widgets.get("transformation_payload").strip()
if not payload_str:
    raise ValueError("transformation_payload is empty")
payload_dict = json.loads(payload_str)

import wisdom_of_crowds.transformations  # noqa: F401  — registers everything
from wisdom_of_crowds.framework import TransformationPayload, run_payload

# Inject secrets into the payload's params where relevant. Secrets stay
# out of the payload string (which is visible in job run history) and
# only enter the process for the run itself.
try:
    dune_api_key = dbutils.secrets.get("wisdom_of_crowds", "dune_api_key")
except Exception:  # noqa: BLE001 — scope/key absent is expected for sources that don't need it
    dune_api_key = ""
if dune_api_key:
    payload_dict.setdefault("params", {})
    payload_dict["params"].setdefault("dune_api_key", dune_api_key)

payload = TransformationPayload.from_dict(payload_dict)
print("running transformation:", payload.transformation_name)
row_counts = run_payload(spark, payload)
print("row counts per output:", row_counts)
