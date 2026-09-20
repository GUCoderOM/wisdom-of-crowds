"""Source registry — the runner's dispatch table.

Register a new source by importing its module and adding an entry here.
Deliberately eager rather than lazy: an import error surfaces at startup,
not on the first ingest run.

The registry keys on the source's ``slug``. The integer FK ``source_id``
that ends up in guesses/wisdom rows lives in
:data:`wisdom_of_crowds.schema.sources.SOURCE_CATALOG`.
"""

from __future__ import annotations

from wisdom_of_crowds.ingest.base import Source
from wisdom_of_crowds.ingest.sources import (
    ecb_spf,
    manifold,
    noaa,
    polymarket,
    spf,
    sweets_jar,
)

SOURCES: dict[str, type[Source]] = {
    sweets_jar.SweetsJarSource.slug:  sweets_jar.SweetsJarSource,
    spf.SPFSource.slug:               spf.SPFSource,
    manifold.ManifoldSource.slug:     manifold.ManifoldSource,
    ecb_spf.ECBSPFSource.slug:        ecb_spf.ECBSPFSource,
    noaa.NOAASource.slug:             noaa.NOAASource,
    polymarket.PolymarketSource.slug: polymarket.PolymarketSource,
}
