# Databricks notebook source
# Single parameterised task for the ``vox_populi`` job.
#
# Everest/DAB convention: one job, one task, run parameters decide which
# transformation runs. That keeps the job history unified, avoids task
# fan-out per source, and makes it possible for parallel agents to trigger
# their own runs against different (phase, source_slug) partitions without
# stepping on each other.
#
# Parameters:
#   phase        - "ingest" | "aggregate" | "end_to_end"
#   source_slug  - registry slug: sweets_jar | spf | manifold | ecb_spf | aaii | noaa
#                  (unused when phase = "aggregate" without a per-source filter)
#   silver_table - fully-qualified silver Delta table
#   gold_table   - fully-qualified gold Delta table
#   sources_table - fully-qualified dimension table (optional; runner derives it)
#   wheel_path   - workspace path to the installed wheel

dbutils.widgets.text("phase",         "ingest")
dbutils.widgets.text("source_slug",   "spf")
dbutils.widgets.text("silver_table",  "vox_populi.core.silver")
dbutils.widgets.text("gold_table",    "vox_populi.core.gold")
dbutils.widgets.text("sources_table", "vox_populi.core.sources")
dbutils.widgets.text("wheel_path",    "")

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

import datetime as dt
import sys

phase          = dbutils.widgets.get("phase").strip().lower()
source_slug    = dbutils.widgets.get("source_slug").strip()
silver_table   = dbutils.widgets.get("silver_table")
gold_table     = dbutils.widgets.get("gold_table")
sources_table  = dbutils.widgets.get("sources_table")

if phase not in {"ingest", "aggregate", "end_to_end"}:
    raise ValueError(
        f"phase must be 'ingest', 'aggregate', or 'end_to_end'; got {phase!r}",
    )

# Ensure catalog + schema exist. Idempotent. Runs once per invocation so
# the deploy step doesn't have to remember to create them separately.
spark.sql("CREATE CATALOG IF NOT EXISTS vox_populi")
spark.sql("CREATE SCHEMA IF NOT EXISTS vox_populi.core")

if phase in {"ingest", "end_to_end"}:
    from wisdom_of_crowds.ingest.runner import main as ingest_main
    sys.argv = [
        "vox-ingest",
        "--source-slug",    source_slug,
        "--silver-output",  silver_table,
        "--sources-output", sources_table,
    ]
    ingest_main()

if phase in {"aggregate", "end_to_end"}:
    from wisdom_of_crowds.aggregate_cli import main as agg_main
    argv = [
        "vox-aggregate",
        "--silver-source", silver_table,
        "--gold-output",   gold_table,
        "--cycle-dt",      dt.date.today().isoformat(),
    ]
    # Aggregating a single source is more efficient (partition pruning);
    # if source_slug is set, hand it through so the aggregator scopes to
    # that (cycle_dt, source_id) partition.
    if source_slug:
        argv += ["--source-slug", source_slug]
    sys.argv = argv
    agg_main()
