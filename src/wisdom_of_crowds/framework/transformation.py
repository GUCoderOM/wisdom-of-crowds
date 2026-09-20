"""The transformation protocol + payload runner.

A transformation is a named unit with declared input keys, output keys,
and a :func:`run` method taking ``dict[str, DataFrame]`` and returning
``dict[str, DataFrame]``. The framework — not the transformation —
handles I/O, filters, partition writes, and MERGE upserts. That keeps
transformations pure functions on DataFrames, easy to unit-test.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_log = logging.getLogger(__name__)


class WriteMode(str, Enum):
    """How the framework writes an output DataFrame.

    * ``OVERWRITE_PARTITION`` — Delta ``overwrite`` with ``replaceWhere``
      scoped to a specific partition (default). Idempotent per partition;
      parallel writes to disjoint partitions are safe.
    * ``MERGE_ON_KEY`` — Delta ``MERGE`` upsert on the declared keys.
      Used for dimension tables like ``sources``.
    * ``APPEND`` — simple append; used only for local Parquet dev.
    """
    OVERWRITE_PARTITION = "overwrite_partition"
    MERGE_ON_KEY        = "merge_on_key"
    APPEND              = "append"


@dataclass(frozen=True)
class OutputSpec:
    """How to write one output of a transformation."""

    write_mode:    WriteMode           = WriteMode.OVERWRITE_PARTITION
    partition_by:  tuple[str, ...]     = ()
    #: Predicate for ``replaceWhere``, with ``{param_name}`` placeholders
    #: filled from the run's ``params``. E.g.
    #: ``"source_id = {source_id} AND cycle_dt = date'{cycle_dt}'"``.
    replace_where: str | None          = None
    #: Merge match keys, for :attr:`WriteMode.MERGE_ON_KEY`.
    merge_keys:    tuple[str, ...]     = ()


class Transformation(Protocol):
    """A named transformation. Every registered transformation implements
    this protocol."""

    name:         str
    input_keys:   tuple[str, ...]           # keys the runner passes in ``inputs``
    output_specs: Mapping[str, OutputSpec]  # keys the runner receives from :func:`run`

    def run(
        self,
        spark:  SparkSession,
        inputs: Mapping[str, DataFrame],
        params: Mapping[str, Any],
    ) -> Mapping[str, DataFrame]:  # pragma: no cover — protocol
        ...


@dataclass(frozen=True)
class TransformationPayload:
    """The exact shape of a job-run payload."""

    transformation_name: str
    inputs:  Mapping[str, str]        = field(default_factory=dict)
    outputs: Mapping[str, str]        = field(default_factory=dict)
    params:  Mapping[str, Any]        = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "TransformationPayload":
        return cls(
            transformation_name=str(d["transformation_name"]),
            inputs=dict(d.get("inputs")  or {}),
            outputs=dict(d.get("outputs") or {}),
            params=dict(d.get("params")  or {}),
        )


# ---------------------------------------------------------------------------
# Registry (populated by :mod:`wisdom_of_crowds.transformations`)
# ---------------------------------------------------------------------------


_REGISTRY: dict[str, Transformation] = {}


def register(t: Transformation) -> Transformation:
    """Register a transformation. Duplicate names raise."""
    if t.name in _REGISTRY:
        raise KeyError(f"transformation_name {t.name!r} already registered")
    _REGISTRY[t.name] = t
    return t


def get_transformation(name: str) -> Transformation:
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown transformation_name {name!r}. Registered: "
            f"{sorted(_REGISTRY)}",
        )
    return _REGISTRY[name]


def registered_names() -> list[str]:
    return sorted(_REGISTRY)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_payload(spark: SparkSession, payload: TransformationPayload) -> dict[str, int]:
    """Dispatch on ``payload.transformation_name`` and execute end-to-end.

    Returns the number of rows written per output key.
    """
    t = get_transformation(payload.transformation_name)

    _log.info("transformation_start", extra={
        "transformation_name": t.name,
        "inputs":  dict(payload.inputs),
        "outputs": dict(payload.outputs),
        "params":  dict(payload.params),
    })

    # Read every input table into a DataFrame keyed by its logical name.
    # Apply partition-column filters from `params` automatically — anywhere
    # the DataFrame carries a column named after a param, only rows matching
    # that param's value survive. That's how the aggregate transformation
    # gets partition pruning without knowing anything about the underlying
    # storage.
    input_dfs: dict[str, DataFrame] = {}
    for key, table in payload.inputs.items():
        if key not in t.input_keys:
            raise KeyError(
                f"{t.name}: unexpected input key {key!r}. "
                f"Declared inputs: {list(t.input_keys)}",
            )
        input_dfs[key] = _apply_partition_filters(_read(spark, table), payload.params)

    missing = set(t.input_keys) - set(input_dfs)
    if missing:
        raise KeyError(f"{t.name}: missing input tables for keys {sorted(missing)}")

    # Run the pure transformation.
    output_dfs = t.run(spark, input_dfs, payload.params)

    # Write every output back to its target table.
    row_counts: dict[str, int] = {}
    for key, df in output_dfs.items():
        if key not in t.output_specs:
            raise KeyError(
                f"{t.name}: transformation returned unknown output key {key!r}. "
                f"Declared outputs: {list(t.output_specs)}",
            )
        if key not in payload.outputs:
            raise KeyError(
                f"{t.name}: payload has no output target for key {key!r}. "
                f"Payload outputs: {list(payload.outputs)}",
            )
        target = payload.outputs[key]
        spec   = t.output_specs[key]
        n      = _write(df, target, spec, payload.params)
        row_counts[key] = n

    _log.info("transformation_complete", extra={
        "transformation_name": t.name,
        "row_counts": row_counts,
    })
    return row_counts


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _is_table_name(target: str) -> bool:
    return ("/" not in target) and (target.count(".") >= 1)


def _read(spark: SparkSession, target: str) -> DataFrame:
    if _is_table_name(target):
        return spark.read.table(target)
    return spark.read.parquet(target)


def _apply_partition_filters(
    df: DataFrame,
    params: Mapping[str, Any],
) -> DataFrame:
    """Filter ``df`` by any ``params`` whose name matches a column name.

    Only applies filters for a small allowlist of columns that are known
    partition keys — matching on any-column would surprise unrelated
    transformations.
    """
    filterable = {"source_id", "cycle_dt"}
    df_cols = set(df.columns)
    out = df
    for k in filterable & df_cols & set(params):
        v = params[k]
        if k == "cycle_dt":
            v = _to_date(v)
        out = out.filter(F.col(k) == F.lit(v))
    return out


def _write(
    df: DataFrame,
    target: str,
    spec: OutputSpec,
    params: Mapping[str, Any],
) -> int:
    if spec.write_mode == WriteMode.MERGE_ON_KEY:
        _merge_upsert(df, target, spec.merge_keys)
    elif spec.write_mode == WriteMode.OVERWRITE_PARTITION:
        _overwrite_partition(df, target, spec, params)
    elif spec.write_mode == WriteMode.APPEND:
        _append(df, target, spec)
    else:  # pragma: no cover
        raise ValueError(f"unknown write_mode {spec.write_mode}")
    return df.count()


def _overwrite_partition(
    df: DataFrame,
    target: str,
    spec: OutputSpec,
    params: Mapping[str, Any],
) -> None:
    is_table = _is_table_name(target)
    partition_by = list(spec.partition_by)

    if is_table:
        # Pre-create the table if it doesn't exist yet. Without this,
        # parallel first-writes race — one wins, the other gets
        # ``TABLE_OR_VIEW_ALREADY_EXISTS`` from ``saveAsTable(mode=overwrite)``.
        # ``CREATE TABLE IF NOT EXISTS`` is idempotent under concurrency.
        _ensure_delta_table(df, target, partition_by)

    if is_table and spec.replace_where:
        where = _fill_placeholders(spec.replace_where, params)
        writer = (
            df.write.format("delta").mode("overwrite")
              .option("replaceWhere", where)
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(target)
    elif is_table:
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(target)
    else:
        writer = df.write.mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(target)


def _ensure_delta_table(
    df: DataFrame,
    target: str,
    partition_by: list[str],
) -> None:
    """Idempotent ``CREATE TABLE IF NOT EXISTS ... USING DELTA``.

    Uses the DataFrame's schema to build the DDL, so every downstream
    write sees a table it can ``replaceWhere`` into — even on the first
    parallel wave against an empty catalog.
    """
    spark = df.sparkSession
    schema_ddl = ", ".join(
        f"{f.name} {f.dataType.simpleString()}"
        + (" NOT NULL" if not f.nullable else "")
        for f in df.schema.fields
    )
    partition_clause = (
        f" PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} ({schema_ddl}) "
        f"USING DELTA{partition_clause}",
    )


def _append(df: DataFrame, target: str, spec: OutputSpec) -> None:
    partition_by = list(spec.partition_by)
    if _is_table_name(target):
        writer = df.write.format("delta").mode("append")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(target)
    else:
        writer = df.write.mode("append")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(target)


def _merge_upsert(df: DataFrame, target: str, merge_keys: tuple[str, ...]) -> None:
    """Delta MERGE upsert. Only supports Delta tables (fully-qualified
    catalog.schema.table). For local Parquet dev, callers should use
    :attr:`WriteMode.APPEND` instead.
    """
    if not _is_table_name(target):
        raise ValueError(
            f"MERGE_ON_KEY write mode requires a Delta table target; got {target!r}",
        )
    if not merge_keys:
        raise ValueError("MERGE_ON_KEY requires merge_keys")

    spark = df.sparkSession
    view = f"_wc_merge_{abs(hash(target))}"
    df.createOrReplaceTempView(view)

    # Create the table if it doesn't exist yet — first-ever run.
    schema_ddl = ", ".join(
        f"{f.name} {f.dataType.simpleString()}"
        + (" NOT NULL" if not f.nullable else "")
        for f in df.schema.fields
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({schema_ddl}) USING DELTA")

    on = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    non_key_cols = [f.name for f in df.schema.fields if f.name not in merge_keys]
    set_clause = ", ".join(f"{c} = s.{c}" for c in non_key_cols) or "(1=1)"
    insert_cols = ", ".join(f.name for f in df.schema.fields)
    insert_vals = ", ".join(f"s.{f.name}" for f in df.schema.fields)

    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON {on}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


def _fill_placeholders(template: str, params: Mapping[str, Any]) -> str:
    """Cheap-and-safe placeholder fill: only accepts identifiers we know
    are partition columns (int or date). Prevents SQL injection via
    param values (which come from an untrusted JSON payload)."""
    safe: dict[str, str] = {}
    for k, v in params.items():
        if k == "cycle_dt":
            safe[k] = _to_date(v).isoformat()
        elif isinstance(v, int):
            safe[k] = str(v)
        elif isinstance(v, str) and v.isidentifier():
            safe[k] = v
        elif isinstance(v, str) and v.replace("-", "").isdigit():
            # ISO date-shaped string — allow, since _to_date-safe cycle_dt
            # above already handles the common case.
            safe[k] = v
        else:
            safe[k] = "" if v is None else str(v)
    return template.format(**safe)


def _to_date(v: Any) -> dt.date:
    if isinstance(v, dt.date):
        return v
    if isinstance(v, str):
        return dt.date.fromisoformat(v)
    raise TypeError(f"cannot coerce {v!r} to date")
