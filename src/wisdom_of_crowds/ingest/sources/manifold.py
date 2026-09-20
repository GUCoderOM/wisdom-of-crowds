"""Manifold Markets ingestion — one row per bet, exhaustive pagination.

Every bet becomes one row in the shared ``answers`` table:

* ``name``           — Manifold ``userId`` of the trader
* ``answer_value``   — the price the trader was buying at (``probAfter``)
* ``answer_outcome`` — YES / NO / the specific answer text for MC markets
* ``weight``         — mana staked (``amount``), useful for weighted aggregations
* ``created_at``     — Manifold's ``createdTime``

Pagination is exhaustive: for each market we keep fetching pages of
``/bets?contractId=X&limit=1000&before=<last_id>`` until Manifold
returns an empty page. No per-market cap; the ingest run captures every
bet the API knows about.

Reference: https://docs.manifold.markets/api
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

_API_BASE = "https://api.manifold.markets/v0"

# Contract types that Manifold returns.
_BINARY     = "BINARY"
_MULTI      = "MULTIPLE_CHOICE"
_FREE_RESP  = "FREE_RESPONSE"
_POLL       = "POLL"
_PSEUDO_NUM = "PSEUDO_NUMERIC"


class ManifoldSource(Source):
    slug = "manifold"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        limit_markets    = int(cfg.params.get("limit_markets", 40))
        page_size        = int(cfg.params.get("bets_page_size", 1000))
        min_bet_count    = int(cfg.params.get("min_bet_count", 5))
        sort             = str(cfg.params.get("sort", "updated-time"))
        include_resolved = bool(cfg.params.get("include_resolved", False))
        per_market_cap   = cfg.params.get("max_bets_per_market")  # None = uncapped

        markets = self._list_markets(limit_markets, sort, include_resolved, min_bet_count)
        _log.info("manifold_markets_listed", extra={"n": len(markets)})

        rows: list[tuple] = []
        for m in markets:
            try:
                self._emit_market(m, cfg, page_size, per_market_cap, rows)
            except Exception as exc:  # noqa: BLE001 — one bad market shouldn't kill the run
                _log.warning("manifold_market_skipped", extra={
                    "market_id": m.get("id"), "slug": m.get("slug"), "err": repr(exc),
                })

        if not rows:
            return spark.createDataFrame([], INGEST_ANSWER_SCHEMA)
        return spark.createDataFrame(rows, INGEST_ANSWER_SCHEMA)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @staticmethod
    def _get_json(path: str, query: dict[str, Any] | None = None,
                  retries: int = 3) -> Any:
        url = f"{_API_BASE}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        req = urllib.request.Request(url, headers={"User-Agent": "wisdom-of-crowds/0.4"})
        last: Exception | None = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 — trusted host
                    return json.loads(r.read().decode())
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                time.sleep(1.5 ** attempt)
        raise RuntimeError(f"Manifold API failed after {retries} attempts: {last}")

    def _list_markets(
        self, limit: int, sort: str, include_resolved: bool, min_bet_count: int,
    ) -> list[dict[str, Any]]:
        pool: list[dict[str, Any]] = []
        cursor: str | None = None
        while len(pool) < limit:
            q: dict[str, Any] = {"limit": min(500, limit * 4), "sort": sort}
            if cursor:
                q["before"] = cursor
            page = self._get_json("/markets", q)
            if not page:
                break
            for m in page:
                if m.get("isResolved") and not include_resolved:
                    continue
                if m.get("outcomeType") not in (_BINARY, _MULTI, _FREE_RESP, _POLL, _PSEUDO_NUM):
                    continue
                if int(m.get("uniqueBettorCount", 0)) < min_bet_count:
                    continue
                pool.append(m)
                if len(pool) >= limit:
                    break
            cursor = page[-1]["id"]
        return pool

    def _fetch_all_bets(
        self, contract_id: str, page_size: int, cap: int | None,
    ) -> Iterable[dict[str, Any]]:
        """Yield every bet on the market, oldest-to-newest not guaranteed.

        Walks Manifold's cursor pagination (``before=<last_bet_id>``) until
        an empty page is returned. ``cap`` (from YAML) stops the walk
        early — leave it unset to fetch everything.
        """
        fetched = 0
        before: str | None = None
        while True:
            q: dict[str, Any] = {"contractId": contract_id, "limit": page_size}
            if before:
                q["before"] = before
            page = self._get_json("/bets", q)
            if not page:
                return
            for b in page:
                yield b
                fetched += 1
                if cap is not None and fetched >= cap:
                    return
            before = page[-1]["id"]
            # If a partial page came back, we've hit the tail.
            if len(page) < page_size:
                return

    def _fetch_market_full(self, contract_id: str) -> dict[str, Any]:
        """Fetch FullMarket via ``/market/{id}`` — LiteMarket omits
        ``answers`` for MULTIPLE_CHOICE / FREE_RESPONSE."""
        return self._get_json(f"/market/{contract_id}")

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
        mid      = market["id"]
        qtype    = market["outcomeType"]
        question = market.get("question") or f"manifold:{mid}"

        if qtype == _BINARY:
            silver_qtype = QuestionType.BINARY
        elif qtype in (_MULTI, _FREE_RESP):
            silver_qtype = QuestionType.CATEGORICAL
        elif qtype == _POLL:
            silver_qtype = QuestionType.POLL_CATEGORICAL
        elif qtype == _PSEUDO_NUM:
            silver_qtype = QuestionType.NUMERIC_GUESSES
        else:
            return  # unsupported

        # POLL markets aren't bet-based — they carry per-option vote counts.
        # We expand each vote into one anonymous row so wisdom-of-crowds
        # aggregation over ``answers`` works uniformly. The count of votes
        # is real; each row represents one actual voter.
        if qtype == _POLL:
            opts = market.get("options") or []
            for opt in opts:
                text  = str(opt.get("text"))
                votes = int(opt.get("votes") or 0)
                for _ in range(votes):
                    rows.append((
                        self.slug,
                        mid,
                        question,
                        silver_qtype,
                        None,          # anonymous poll vote
                        None,          # answer_value
                        text,          # answer_outcome
                        None,          # weight
                        None,          # created_at
                        cfg.cycle_ts,
                    ))
            return

        # PSEUDO_NUMERIC needs the market's [min, max] to map probability
        # -> value. Fetch min/max once up front.
        pn_mn = pn_mx = None
        pn_log = False
        if qtype == _PSEUDO_NUM:
            pn_mn  = _f(market.get("min"))
            pn_mx  = _f(market.get("max"))
            pn_log = bool(market.get("isLogScale"))

        # For categorical markets, hydrate the FullMarket if answers are
        # missing so bet.outcome text can be resolved.
        answers_by_id: dict[str, str] = {}
        if qtype in (_MULTI, _FREE_RESP):
            ans = market.get("answers")
            if not ans:
                try:
                    ans = self._fetch_market_full(mid).get("answers") or []
                except Exception as exc:  # noqa: BLE001
                    _log.warning("manifold_market_hydrate_failed", extra={
                        "market_id": mid, "err": repr(exc),
                    })
                    ans = []
            for a in ans or []:
                # Skip resolved sub-answers: their probability is pinned
                # at 1.0/0.0 which is a settled fact, not a forecast.
                if a.get("resolution"):
                    continue
                aid = a.get("id")
                if aid:
                    answers_by_id[aid] = str(a.get("text"))

        # Walk every bet.
        for b in self._fetch_all_bets(mid, page_size, per_market_cap):
            prob_after = _f(b.get("probAfter"))
            created_ts = _epoch_ms_to_ts(b.get("createdTime"))

            if qtype == _PSEUDO_NUM:
                value = _map_prob_to_value(prob_after, pn_mn, pn_mx, pn_log) if prob_after is not None and pn_mn is not None and pn_mx is not None else None
                outcome = None
            elif qtype == _BINARY:
                value   = prob_after            # fill probability
                outcome = b.get("outcome")      # "YES" / "NO"
            else:  # MULTI / FREE_RESP
                # bet.outcome is an answer id; map to its text.
                aid = b.get("outcome")
                outcome_text = answers_by_id.get(aid) if aid else None
                if outcome_text is None:
                    # Answer is resolved-and-filtered OR unknown — skip.
                    continue
                value   = prob_after            # fill probability at the answer
                outcome = outcome_text

            rows.append((
                self.slug,
                mid,
                question,
                silver_qtype,
                b.get("userId"),           # name
                value,                     # answer_value
                outcome,                   # answer_outcome
                _f(b.get("amount")),       # weight — mana staked
                created_ts,
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


def _epoch_ms_to_ts(ms: Any) -> dt.datetime | None:
    if ms is None:
        return None
    try:
        return dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc)
    except (TypeError, ValueError):
        return None


def _map_prob_to_value(p: float, mn: float, mx: float, log_scale: bool) -> float:
    if log_scale:
        return (mx - mn + 1.0) ** p + mn - 1.0
    return mn + (mx - mn) * p
