"""The ingest ``Source`` contract.

A ``Source`` knows how to turn the raw payload of one external system into
silver-shaped rows. It knows nothing about where silver lives — the runner
handles I/O — so sources are trivial to unit-test.
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
        source_id:  short slug, e.g. ``"sweets_jar"``, ``"spf"``.
        cycle_ts:   timestamp of this ingest run; embedded in every row.
        params:     source-specific parameter dict, loaded from
                    ``config/sources/<source_id>.yaml``.
    """
    source_id: str
    cycle_ts:  dt.datetime
    params:    Mapping[str, Any]


class Source(abc.ABC):
    """Abstract base class for ingestion sources."""

    #: Short slug. Every subclass must set this; the registry keys on it.
    source_id: str = ""

    @abc.abstractmethod
    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        """Return a DataFrame conforming to
        :data:`wisdom_of_crowds.schema.silver.SILVER_SCHEMA`.

        Implementations are responsible for downloading / reading the source
        payload and shaping it into silver. No writes here — the runner
        owns the destination.
        """
