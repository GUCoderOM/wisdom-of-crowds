"""Polymarket ingestion — via Dune Analytics (`polymarket_polygon.market_trades`).

Polymarket's own ``data-api.polymarket.com/trades`` endpoint refuses
``offset`` values past ~10,500. That's the API's cap, not ours, so the
per-market row count on hot markets was right-censored at 10,500 no
matter how aggressively we paginated.

Dune Analytics indexes every on-chain Polymarket trade under
``polymarket_polygon.market_trades`` (columns: ``condition_id``,
``question``, ``token_outcome``, ``price``, ``amount``, ``shares``,
``maker``, ``taker``, ``is_taker_side``, ``block_time``, ``unique_key``,
...). Querying via the Dune API lifts the cap entirely.

Flow:

1. Pull the market universe from the Gamma API (same as before — cheap,
   uncensored on the market-list side).
2. Run one Dune SQL query for those condition_ids, filtered to the
   configured lookback window.
3. Emit one row per trade into ``answers``:

   * ``name``           — the trader's wallet address (taker if the
     trade was taker-side, else maker)
   * ``answer_value``   — the fill ``price``
   * ``answer_outcome`` — ``token_outcome`` (the specific side the
     trader bought)
   * ``weight``         — ``amount`` (USD notional)
   * ``created_at``     — ``block_time``

Requires ``DUNE_API_KEY`` in ``cfg.params`` (or the process env). The
Dune Plus tier is required — the source creates a fresh query per run
so filters can be pushed into SQL. Free tier can't create queries via
the API; if the key is a free-tier key, this raises at run time with a
clear error, and callers can fall back to the Gamma-only path by
setting ``dune_disabled: true`` in the YAML.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
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
_DUNE_BASE  = "https://api.dune.com/api/v1"

_DUNE_QUERY_NAME = "wisdom_of_crowds:polymarket_trades"

# Batch size when embedding condition IDs in a Dune SQL IN(...) clause.
# Dune has a hard cap on SQL length; 200 IDs at ~66 chars each = ~13 KB
# is well within limits and keeps each query fast.
_IDS_PER_QUERY = 200

# Dune execution poll config.
_POLL_INTERVAL_S = 5
_POLL_ATTEMPTS   = 120  # up to 10 min per query


class PolymarketSource(Source):
    slug = "polymarket"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        limit_markets   = int(cfg.params.get("limit_markets", 100))
        min_trade_count = int(cfg.params.get("min_trade_count", 5))
        include_closed  = bool(cfg.params.get("include_closed", False))
        # How far back to fetch trades from. None = no lower bound.
        lookback_days   = cfg.params.get("lookback_days", 365)

        api_key = cfg.params.get("dune_api_key") or os.environ.get("DUNE_API_KEY")
        if not api_key:
            # Hard failure — without the key we can't ingest, and a silent
            # empty batch would overwrite the previous cycle's partition
            # with nothing. Better to fail the run and preserve prior data.
            raise RuntimeError(
                "DUNE_API_KEY not set. Put it in the wisdom_of_crowds/dune_api_key "
                "secret scope or pass in params.dune_api_key.",
            )

        markets = self._list_markets(limit_markets, include_closed, min_trade_count)
        _log.info("polymarket_markets_listed", extra={"n": len(markets)})
        if not markets:
            # Genuine "nothing to ingest" — market universe is empty.
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)

        condition_ids = [m["conditionId"] for m in markets if m.get("conditionId")]
        cid_to_market = {m["conditionId"]: m for m in markets if m.get("conditionId")}

        # Any Dune failure propagates — the framework aborts the run
        # before touching the partition, so prior data stays intact.
        rows: list[tuple] = []
        for batch in _batches(condition_ids, _IDS_PER_QUERY):
            for trade in self._fetch_trades_via_dune(api_key, batch, lookback_days):
                self._emit_trade(trade, cid_to_market, cfg, rows)

        if not rows:
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    # ------------------------------------------------------------------
    # Gamma — market universe
    # ------------------------------------------------------------------

    @staticmethod
    def _get_json(base: str, path: str, query: dict[str, Any] | None = None,
                  retries: int = 3, headers: dict[str, str] | None = None,
                  method: str = "GET", body: bytes | None = None) -> Any:
        url = f"{base}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query, safe="")
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "wisdom-of-crowds/0.5", **(headers or {})},
            data=body,
            method=method,
        )
        last: Exception | None = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310 — trusted host
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                # 4xx (except 429) is not a transient failure.
                if e.code != 429 and 400 <= e.code < 500:
                    body_text = e.read().decode("utf-8", errors="replace")[:300]
                    raise RuntimeError(f"HTTP {e.code} {url}: {body_text}") from e
                last = e
                time.sleep(1.5 ** attempt)
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                time.sleep(1.5 ** attempt)
        raise RuntimeError(f"HTTP failed after {retries} attempts: {last}")

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

    # ------------------------------------------------------------------
    # Dune — per-trade fidelity
    # ------------------------------------------------------------------

    def _fetch_trades_via_dune(
        self,
        api_key:       str,
        condition_ids: list[str],
        lookback_days: int | None,
    ) -> Iterable[dict[str, Any]]:
        """Create a Dune query for this batch of condition IDs, execute it,
        poll to completion, yield trade rows, then archive the query so
        every run cleans up after itself instead of leaving trash behind
        in the Dune account."""
        sql = _build_trades_sql(condition_ids, lookback_days)
        _log.info("polymarket_dune_query_create", extra={"n_ids": len(condition_ids)})
        create_resp = self._get_json(
            _DUNE_BASE, "/query",
            headers={"X-DUNE-API-KEY": api_key, "Content-Type": "application/json"},
            method="POST",
            body=json.dumps({
                "name":        _DUNE_QUERY_NAME,
                "description": "auto-generated by wisdom-of-crowds ingest",
                "query_sql":   sql,
                "is_private":  True,
            }).encode(),
        )
        query_id = create_resp.get("query_id") if isinstance(create_resp, dict) else None
        if not query_id:
            raise RuntimeError(f"Dune query creation returned no id: {create_resp}")

        try:
            exec_resp = self._get_json(
                _DUNE_BASE, f"/query/{query_id}/execute",
                headers={"X-DUNE-API-KEY": api_key},
                method="POST",
            )
            execution_id = exec_resp.get("execution_id") if isinstance(exec_resp, dict) else None
            if not execution_id:
                raise RuntimeError(f"Dune execute returned no id: {exec_resp}")

            # Poll to completion.
            for _ in range(_POLL_ATTEMPTS):
                time.sleep(_POLL_INTERVAL_S)
                status = self._get_json(
                    _DUNE_BASE, f"/execution/{execution_id}/status",
                    headers={"X-DUNE-API-KEY": api_key},
                )
                state = status.get("state", "") if isinstance(status, dict) else ""
                if state == "QUERY_STATE_COMPLETED":
                    break
                if state == "QUERY_STATE_FAILED":
                    raise RuntimeError(
                        f"Dune query {query_id} failed: {status.get('error')}",
                    )
            else:
                raise TimeoutError(f"Dune query {query_id} did not complete in time")

            # Paginate result rows. Dune returns TWO counts in metadata:
            # ``row_count`` = rows on this page, ``total_row_count`` =
            # rows in the whole execution. The stop condition is
            # ``total_row_count`` — reading ``row_count`` caps at page
            # size (verified live: metadata carries both).
            page_size   = 25000
            next_offset = 0
            while True:
                page = self._get_json(
                    _DUNE_BASE, f"/execution/{execution_id}/results",
                    query={"limit": page_size, "offset": next_offset},
                    headers={"X-DUNE-API-KEY": api_key},
                )
                result = page.get("result") or {}
                rows = result.get("rows") or []
                if not rows:
                    return
                for r in rows:
                    yield r
                meta = result.get("metadata") or {}
                total_rows = int(meta.get("total_row_count") or 0)
                next_offset += len(rows)
                if total_rows and next_offset >= total_rows:
                    return
                # If Dune didn't populate total_row_count (rare), fall
                # back to "partial page means done".
                if not total_rows and len(rows) < page_size:
                    return
        finally:
            # Archive the auto-created query so every run leaves no trace.
            try:
                self._get_json(
                    _DUNE_BASE, f"/query/{query_id}/archive",
                    headers={"X-DUNE-API-KEY": api_key},
                    method="POST",
                )
            except Exception as exc:  # noqa: BLE001 — cleanup best-effort
                _log.warning("polymarket_dune_archive_failed", extra={
                    "query_id": query_id, "err": repr(exc),
                })

    def _emit_trade(
        self,
        trade:         dict[str, Any],
        cid_to_market: dict[str, dict[str, Any]],
        cfg:           SourceConfig,
        rows:          list[tuple],
    ) -> None:
        cid = trade.get("condition_id")
        market = cid_to_market.get(cid)
        if market is None:
            return  # trade for a market not in our universe

        outcomes = _parse_json_list(market.get("outcomes"))
        qtype = (
            QuestionType.BINARY if len(outcomes) == 2 else QuestionType.CATEGORICAL
        )
        question = market.get("question") or trade.get("question") or f"polymarket:{cid}"

        # The trader is the taker when is_taker_side, else the maker.
        is_taker = bool(trade.get("is_taker_side"))
        trader = trade.get("taker") if is_taker else trade.get("maker")

        rows.append((
            self.slug,
            cid,
            question,
            qtype,
            trader,                                     # name
            _f(trade.get("price")),                     # answer_value
            trade.get("token_outcome"),                 # answer_outcome
            _f(trade.get("amount")),                    # weight
            _parse_dune_time(trade.get("block_time")),  # created_at
            cfg.cycle_ts,
        ))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _build_trades_sql(condition_ids: list[str], lookback_days: int | None) -> str:
    """Build the trades SELECT for a batch of condition IDs.

    ``polymarket_polygon.market_trades.condition_id`` is ``varbinary`` on
    Dune, not a string — Trino refuses ``varchar`` literals against a
    ``varbinary`` column. Wrapping each id in ``from_hex('...')`` (with
    the ``0x`` prefix stripped) coerces cleanly."""
    hex_ids = ",".join(f"from_hex('{cid.removeprefix('0x')}')" for cid in condition_ids)
    time_clause = ""
    if lookback_days is not None:
        time_clause = f"\n  AND block_time > NOW() - INTERVAL '{int(lookback_days)}' DAY"
    return (
        "SELECT condition_id, question, token_outcome, price, amount, shares, "
        "maker, taker, is_taker_side, block_time, unique_key\n"
        "FROM polymarket_polygon.market_trades\n"
        f"WHERE condition_id IN ({hex_ids}){time_clause}\n"
        "ORDER BY block_time DESC"
    )


def _batches(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


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


def _parse_dune_time(v: Any) -> dt.datetime | None:
    """Dune returns times as ``'2026-09-20 03:59:21.000 UTC'``."""
    if not isinstance(v, str) or not v:
        return None
    try:
        # Strip trailing ' UTC'
        s = v.replace(" UTC", "").strip()
        return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None
