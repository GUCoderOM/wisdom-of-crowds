"""Polymarket ingestion.

Polymarket is a real-money prediction market on Polygon. Two public APIs
we lean on, both key-less:

* Gamma API (``https://gamma-api.polymarket.com/markets``) — market
  metadata + current outcome prices + condition IDs.
* Data API (``https://data-api.polymarket.com/trades``) — every individual
  trade against a market's condition ID, with the trader's on-chain
  proxy-wallet address, size (USD), fill price, side, and timestamp.

This source writes two things per run:

1. **Aggregate ``guesses`` rows** — one per market. Polymarket markets are
   binary (Yes/No) or categorical (one of N tokens), so the crowd's
   answer is the current market price per outcome, same shape as
   :mod:`wisdom_of_crowds.ingest.sources.manifold` produces for its
   BINARY / MULTIPLE_CHOICE markets.
2. **Individual trades** — one row per trade, written to the shared
   ``wisdom_of_crowds.core.bets`` table (schema:
   :mod:`wisdom_of_crowds.schema.bets`). Each row is one trader's revealed
   probability at the moment they bought.

Reference: https://docs.polymarket.com
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.bets import BETS_SCHEMA
from wisdom_of_crowds.schema.guesses import INGEST_SCHEMA, QuestionType
from wisdom_of_crowds.schema.sources import get_meta

_log = logging.getLogger(__name__)

_GAMMA_BASE = "https://gamma-api.polymarket.com"
_DATA_BASE  = "https://data-api.polymarket.com"


class PolymarketSource(Source):
    slug = "polymarket"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        limit_markets       = int(cfg.params.get("limit_markets", 100))
        limit_trades        = int(cfg.params.get("limit_trades_per_market", 500))
        min_trade_count     = int(cfg.params.get("min_trade_count", 5))
        include_closed      = bool(cfg.params.get("include_closed", False))
        bets_table          = cfg.params.get("bets_output")

        markets = self._list_markets(limit_markets, include_closed, min_trade_count)
        _log.info("polymarket_markets_listed", extra={"n": len(markets)})

        guess_rows: list[tuple] = []
        bet_rows:   list[tuple] = []

        source_pk = get_meta(self.slug).source_id
        cycle_dt  = cfg.cycle_ts.date()

        for m in markets:
            try:
                self._emit_market(
                    m, cfg, limit_trades, guess_rows, bet_rows,
                    source_pk=source_pk, cycle_dt=cycle_dt,
                )
            except Exception as exc:  # noqa: BLE001 — one bad market shouldn't kill the run
                _log.warning("polymarket_market_skipped", extra={
                    "condition_id": m.get("conditionId"),
                    "slug": m.get("slug"),
                    "err": repr(exc),
                })

        if bet_rows and bets_table:
            bets_df = spark.createDataFrame(bet_rows, BETS_SCHEMA)
            self._write_bets(bets_df, bets_table, source_pk=source_pk, cycle_dt=cycle_dt)

        if not guess_rows:
            return spark.createDataFrame([], INGEST_SCHEMA)
        return spark.createDataFrame(guess_rows, INGEST_SCHEMA)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @staticmethod
    def _get_json(base: str, path: str, query: dict[str, Any] | None = None,
                  retries: int = 3) -> Any:
        url = f"{base}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query, safe="")
        req = urllib.request.Request(
            url, headers={"User-Agent": "wisdom-of-crowds/0.3"},
        )
        last: Exception | None = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 — trusted host
                    return json.loads(r.read().decode())
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                time.sleep(1.5 ** attempt)
        raise RuntimeError(f"Polymarket API failed after {retries} attempts: {last}")

    def _list_markets(self, limit: int, include_closed: bool,
                      min_trade_count: int) -> list[dict[str, Any]]:
        """Return up to ``limit`` markets from Gamma, filtered client-side.

        Gamma paginates via ``offset``; response is a JSON array. Filter by
        recent activity (``active=true``) and non-negligible participation.
        """
        pool: list[dict[str, Any]] = []
        offset = 0
        page_size = min(500, max(50, limit * 2))
        while len(pool) < limit:
            q: dict[str, Any] = {
                "limit": page_size,
                "offset": offset,
                "active": "true",
                "archived": "false",
                "order": "volume24hr",
                "ascending": "false",
            }
            if not include_closed:
                q["closed"] = "false"
            page = self._get_json(_GAMMA_BASE, "/markets", q)
            if not page:
                break
            for m in page:
                # A Polymarket "market" has a conditionId and one or more outcomes
                # (JSON-encoded string lists in `outcomes` and `outcomePrices`).
                if not m.get("conditionId"):
                    continue
                outcomes = _parse_json_list(m.get("outcomes"))
                if len(outcomes) < 2:
                    continue
                # Skip low-participation markets — cheap client-side filter.
                if int(m.get("volumeNum") or 0) < min_trade_count:
                    continue
                pool.append(m)
                if len(pool) >= limit:
                    break
            offset += page_size
        return pool

    def _fetch_trades(self, condition_id: str, limit: int) -> list[dict[str, Any]]:
        return self._get_json(_DATA_BASE, "/trades", {
            "market": condition_id,
            "limit":  limit,
            "takerOnly": "false",
        })

    # ------------------------------------------------------------------
    # per-market emission
    # ------------------------------------------------------------------

    def _emit_market(
        self,
        market: dict[str, Any],
        cfg: SourceConfig,
        limit_trades: int,
        guess_rows: list[tuple],
        bet_rows: list[tuple],
        *,
        source_pk: int,
        cycle_dt: dt.date,
    ) -> None:
        cid       = market["conditionId"]
        mslug     = market.get("slug")
        question  = market.get("question") or f"polymarket:{cid}"
        outcomes  = _parse_json_list(market.get("outcomes"))
        prices    = [_f(p) or 0.0 for p in _parse_json_list(market.get("outcomePrices"))]
        end_ms    = _parse_iso(market.get("endDate"))
        is_res    = bool(market.get("closed") or market.get("resolved"))
        volume    = _f(market.get("volumeNum") or market.get("volume"))
        resolved_outcome = None
        if is_res and outcomes and prices:
            # Polymarket sets the winning token's price to 1.0 at resolution.
            top = max(range(len(prices)), key=lambda i: prices[i])
            if prices[top] > 0.99:
                resolved_outcome = outcomes[top]

        # Fetch trades. Each trade -> one row in the shared bets table.
        trades = self._fetch_trades(cid, limit_trades)
        for t in trades:
            ts_epoch = t.get("timestamp")
            created_ts = (
                dt.datetime.fromtimestamp(int(ts_epoch), tz=dt.timezone.utc)
                if ts_epoch else None
            )
            price = _f(t.get("price"))
            bet_rows.append((
                source_pk,
                self.slug,
                cid,                                # market_id (conditionId)
                mslug,
                t.get("transactionHash"),           # bet_id — unique per trade
                t.get("proxyWallet"),               # user_id — trader wallet
                _f(t.get("size")),                  # amount (USD notional)
                None,                               # shares — Polymarket doesn't expose separately
                t.get("outcome"),                   # YES / NO / answer text
                price,                              # price — fill probability
                None,                               # prob_before — not published
                price,                              # prob_after — approx = fill price
                None,                               # is_filled — trades are already filled
                created_ts,
                cfg.cycle_ts,
                cycle_dt,
            ))

        # Aggregate row. Polymarket markets are either binary (2 outcomes) or
        # categorical (N outcomes) — same shape as Manifold's BINARY/MULTI.
        qtype = QuestionType.BINARY if len(outcomes) == 2 else QuestionType.CATEGORICAL

        guess_rows.append((
            self.slug,
            cid,
            question,
            qtype,
            None,                       # guesses (numeric guesses not applicable)
            outcomes,
            prices,
            None,                       # trader_count — not directly exposed
            volume,
            end_ms,
            is_res,
            resolved_outcome,
            None,                       # resolved_value — categorical, no scalar
            cfg.cycle_ts,
        ))

    # ------------------------------------------------------------------
    # bets writer
    # ------------------------------------------------------------------

    @staticmethod
    def _write_bets(
        df: DataFrame,
        target: str,
        *,
        source_pk: int,
        cycle_dt: dt.date,
    ) -> None:
        """Write to the shared ``bets`` table, scoped to this source's
        (cycle_dt, source_id) partition — so parallel writes from Manifold
        etc. don't collide."""
        is_table = ("/" not in target) and (target.count(".") >= 1)
        replace_where = (
            f"source_id = {source_pk} AND cycle_dt = date'{cycle_dt.isoformat()}'"
        )
        if is_table:
            (
                df.write.format("delta").mode("overwrite")
                  .option("replaceWhere", replace_where)
                  .partitionBy("cycle_dt", "source_id")
                  .saveAsTable(target)
            )
        else:
            (
                df.write.mode("overwrite")
                  .partitionBy("cycle_dt", "source_id").parquet(target)
            )
        _log.info("polymarket_bets_written", extra={"target": target, "rows": df.count()})


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _f(x: Any) -> float | None:
    """Best-effort float cast; returns None for null/nan/non-numeric."""
    if x is None:
        return None
    try:
        v = float(x)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _parse_json_list(v: Any) -> list[Any]:
    """Polymarket returns some list-shaped fields as JSON-encoded strings."""
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


def _parse_iso(v: Any) -> dt.datetime | None:
    if not isinstance(v, str):
        return None
    try:
        return dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
