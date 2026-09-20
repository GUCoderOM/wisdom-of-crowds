"""Generic ingest layer.

One job, N sources. Each source implements :class:`base.Source` and lands
rows in the canonical silver schema. The source to run is chosen at runtime
by ``source_id``; per-source parameters come from a YAML config file so
adding a new source never requires a code change to the runner.
"""

from wisdom_of_crowds.ingest.base import Source  # noqa: F401
from wisdom_of_crowds.ingest.registry import SOURCES  # noqa: F401
