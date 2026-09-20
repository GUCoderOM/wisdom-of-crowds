# Databricks notebook source
# Thin Databricks wrapper around the ``vox-aggregate`` console script.
# See ingest_task.py for why this installs the wheel in-notebook.

dbutils.widgets.text("silver_table", "workspace.default.vox_populi_silver")
dbutils.widgets.text("gold_table",   "workspace.default.vox_populi_gold")
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

from wisdom_of_crowds.aggregate_cli import main

sys.argv = [
    "vox-aggregate",
    "--silver-source", dbutils.widgets.get("silver_table"),
    "--gold-output",   dbutils.widgets.get("gold_table"),
]
main()
