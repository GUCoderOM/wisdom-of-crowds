# Databricks notebook source
# Thin Databricks wrapper around the ``vox-ingest`` console script.
#
# Runs as a notebook rather than a python_wheel_task for two reasons:
# serverless reports only a generic "error importing the Python wheel" when a
# wheel task fails, and a non-empty ``dependencies`` list on a serverless
# environment restarts the Python REPL underneath a running notebook
# ("notebook command received after detach"). Installing the wheel in-notebook
# and then restarting Python explicitly avoids both.
#
# ``wheel_path`` is supplied by the job from ${workspace.artifact_path} so no
# user- or target-specific path is baked into this file.

dbutils.widgets.text("source_id",    "spf")
dbutils.widgets.text("silver_table", "workspace.default.vox_populi_silver")
dbutils.widgets.text("wheel_path",   "")

# COMMAND ----------

import subprocess
import sys

# Surface pip's own stderr: a bare check_call reports only "exit status 1",
# which hides the actual reason (wrong Requires-Python, missing file, ...).
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

import sys

from wisdom_of_crowds.ingest.runner import main

sys.argv = [
    "vox-ingest",
    "--source-id",     dbutils.widgets.get("source_id"),
    "--silver-output", dbutils.widgets.get("silver_table"),
]
main()
