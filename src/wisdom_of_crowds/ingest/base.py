"""The ingest ``Source`` contract.

A ``Source`` knows how to turn the raw payload of one external system into
:data:`~wisdom_of_crowds.schema.silver.INGEST_SCHEMA`-shaped rows. It knows
nothing about the integer ``source_id``, the ``cycle_dt`` partition key,
or where silver lives — the runner handles all of that.

Every subclass sets ``slug`` (a human/URL-friendly identifier). The runner
looks up the integer ``source_id`` from
:data:`~wisdom_of_crowds.schema.sources.SOURCE_CATALOG` at write time and
foreign-keys every row to the ``vox_populi_sources`` dimension table.
"""

from __future__ import annotations

import abc
import datetime as dt
from dataclasses import dataclass
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class SourceConfig:
    """Everything the runner passes to a source at execution time.

    Attributes:
        slug:      short slug, e.g. ``"sweets_jar"``, ``"spf"``. Registry key.
        cycle_ts:  timestamp of this ingest run; embedded in every row.
        params:    source-specific parameter dict, loaded from
                   ``config/sources/<slug>.yaml``.
    """
    slug:     str
    cycle_ts: dt.datetime
    params:   Mapping[str, Any]

    # Backwards-compat alias — some sources still read cfg.source_id.
    @property
    def source_id(self) -> str:  # noqa: D401 — historical
        """Deprecated alias for :attr:`slug`. New code should use ``slug``."""
        return self.slug


class Source(abc.ABC):
    """Abstract base class for ingestion sources."""

    #: Human/URL-friendly identifier. Registry key. Every subclass sets it.
    slug: str = ""

    @abc.abstractmethod
    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        """Return a DataFrame conforming to
        :data:`wisdom_of_crowds.schema.silver.INGEST_SCHEMA` (no
        ``source_id`` or ``cycle_dt`` — the runner adds those).

        Implementations are responsible for downloading / reading the source
        payload and shaping it into the ingest schema. No writes here — the
        runner owns the destination.
        """
