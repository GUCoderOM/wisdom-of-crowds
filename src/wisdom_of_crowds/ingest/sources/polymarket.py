"""Polymarket ingestion — one row per trade, exhaustive pagination.

Every trade becomes one row in the shared ``answers`` table:

* ``name``           — trader's proxy wallet address (``NULL`` if absent)
* ``answer_value``   — fill probability, i.e. the price the trader paid
* ``answer_outcome`` — which outcome they bought (``YES`` / ``NO`` / an answer text)
* ``weight``         — trade size in USD notional
* ``created_at``     — the trade's on-chain timestamp

Pagination is exhaustive: for each market we walk
``/trades?market=<conditionId>&offset=N`` until an empty (or partial)
page returns. No per-market cap; the ingest run captures every trade
the Data API knows about.

APIs:
  * ``https://gamma-api.polymarket.com/markets`` — market metadata
  * ``https://data-api.polymarket.com/trades``   — individual trades
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.answers import INGEST_ANSWER_SCHEMA, QuestionType

_log = logging.getLogger(__name__)

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_DATA_BASE  = "https://data-api.polymarket.com"


class PolymarketSource(Source):
    slug = "polymarket"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        limit_markets    = int(cfg.params.get("limit_markets", 100))
        page_size        = int(cfg.params.get("trades_page_size", 500))
        min_trade_count  = int(cfg.params.get("min_trade_count", 5))
        include_closed   = bool(cfg.params.get("include_closed", False))
        per_market_cap   = cfg.params.get("max_trades_per_market")  # None = uncapped

        markets = self._list_markets(limit_markets, include_closed, min_trade_count)
        _log.info("polymarket_markets_listed", extra={"n": len(markets)})

        rows: list[tuple] = []
        for m in markets:
            try:
                self._emit_market(m, cfg, page_size, per_market_cap, rows)
            except Exception as exc:  # noqa: BLE001
                _log.warning("polymarket_market_skipped", extra={
                    "condition_id": m.get("conditionId"),
                    "slug":         m.get("slug"),
                    "err":          repr(exc),
                })

        if not rows:
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @staticmethod
    def _get_json(base: str, path: str, query: dict[str, Any] | None = None,
                  retries: int = 3) -> Any:
        """GET + parse JSON with exponential backoff.

        HTTP 400 is treated as a hard end-of-data signal: Polymarket's
        ``/trades`` endpoint returns 400 for ``offset`` values past its
        internal cap (empirically ~10,500). Callers use this to stop
        pagination cleanly rather than fail the whole market.
        """
        url = f"{base}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query, safe="")
        req = urllib.request.Request(url, headers={"User-Agent": "wisdom-of-crowds/0.5"})
        last: Exception | None = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 — trusted host
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                if e.code == 400:
                    # Past the pagination ceiling — treat as empty page.
                    return []
                last = e
                time.sleep(1.5 ** attempt)
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                time.sleep(1.5 ** attempt)
        raise RuntimeError(f"Polymarket API failed after {retries} attempts: {last}")

    def _list_markets(self, limit: int, include_closed: bool,
                      min_trade_count: int) -> list[dict[str, Any]]:
        pool: list[dict[str, Any]] = []
        offset = 0
        page_size = min(500, max(50, limit * 2))
        while len(pool) < limit:
            q: dict[str, Any] = {
                "limit":     page_size,
                "offset":    offset,
                "active":    "true",
                "archived":  "false",
                "order":     "volume24hr",
                "ascending": "false",
            }
            if not include_closed:
                q["closed"] = "false"
            page = self._get_json(_GAMMA_BASE, "/markets", q)
            if not page:
                break
            for m in page:
                if not m.get("conditionId"):
                    continue
                outcomes = _parse_json_list(m.get("outcomes"))
                if len(outcomes) < 2:
                    continue
                if int(m.get("volumeNum") or 0) < min_trade_count:
                    continue
                pool.append(m)
                if len(pool) >= limit:
                    break
            offset += page_size
        return pool

    def _fetch_all_trades(
        self, condition_id: str, page_size: int, cap: int | None,
    ) -> Iterable[dict[str, Any]]:
        fetched = 0
        offset  = 0
        while True:
            page = self._get_json(_DATA_BASE, "/trades", {
                "market":    condition_id,
                "limit":     page_size,
                "offset":    offset,
                "takerOnly": "false",
            })
            if not page:
                return
            for t in page:
                yield t
                fetched += 1
                if cap is not None and fetched >= cap:
                    return
            offset += page_size
            if len(page) < page_size:
                return

    # ------------------------------------------------------------------
    # per-market emission
    # ------------------------------------------------------------------

    def _emit_market(
        self,
        market:         dict[str, Any],
        cfg:            SourceConfig,
        page_size:      int,
        per_market_cap: int | None,
        rows:           list[tuple],
    ) -> None:
        cid       = market["conditionId"]
        question  = market.get("question") or f"polymarket:{cid}"
        outcomes  = _parse_json_list(market.get("outcomes"))
        silver_qtype = (
            QuestionType.BINARY if len(outcomes) == 2 else QuestionType.CATEGORICAL
        )

        for t in self._fetch_all_trades(cid, page_size, per_market_cap):
            price = _f(t.get("price"))
            rows.append((
                self.slug,
                cid,
                question,
                silver_qtype,
                t.get("proxyWallet"),         # name — trader wallet (nullable)
                price,                        # answer_value — fill probability
                t.get("outcome"),             # answer_outcome — YES / NO / answer text
                _f(t.get("size")),            # weight — USD notional
                _epoch_to_ts(t.get("timestamp")),
                cfg.cycle_ts,
            ))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _f(x: Any) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _parse_json_list(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _epoch_to_ts(ts: Any) -> dt.datetime | None:
    if ts is None:
        return None
    try:
        return dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc)
    except (TypeError, ValueError):
        return None
