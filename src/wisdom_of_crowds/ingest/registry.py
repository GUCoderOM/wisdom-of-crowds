"""Source registry — the runner's dispatch table.

Register a new source by importing its module and adding an entry here.
Deliberately eager rather than lazy: an import error surfaces at startup,
not on the first ingest run.
"""

from __future__ import annotations

from wisdom_of_crowds.ingest.base import Source
from wisdom_of_crowds.ingest.sources import spf, sweets_jar

SOURCES: dict[str, type[Source]] = {
    sweets_jar.SweetsJarSource.source_id: sweets_jar.SweetsJarSource,
    spf.SPFSource.source_id:             spf.SPFSource,
}
