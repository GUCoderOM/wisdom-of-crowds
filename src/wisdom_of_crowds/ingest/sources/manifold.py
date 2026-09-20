"""Manifold Markets ingestion.

Manifold is a play-money prediction market with a fully public API at
``https://api.manifold.markets/v0``. It exposes **individual bets** per
market, which is the closest public equivalent to the "everyone writes a
number" shape Galton studied — every bet carries the probability the trader
was buying at, which is that trader's revealed estimate at that moment.

This source ingests two things per run:

1. **Aggregate ``guesses`` rows** — one per market, dispatched to the right
   ``question_type`` (``binary`` / ``categorical`` / ``poll_categorical`` /
   ``numeric_guesses``).
2. **Individual bets** — one row per bet, written to the shared
   ``wisdom_of_crowds.core.bets`` table (schema:
   :mod:`wisdom_of_crowds.schema.bets`). This is the per-trader fidelity
   behind each aggregate row.

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
from pyspark.sql.types import ArrayType, DoubleType

from wisdom_of_crowds.ingest.base import Source, SourceConfig
from wisdom_of_crowds.schema.bets import BETS_SCHEMA
from wisdom_of_crowds.schema.guesses import INGEST_SCHEMA, QuestionType
from wisdom_of_crowds.schema.sources import get_meta

_log = logging.getLogger(__name__)

_API_BASE = "https://api.manifold.markets/v0"

# Contract types that Manifold returns.
_BINARY       = "BINARY"
_MULTI        = "MULTIPLE_CHOICE"
_FREE_RESP    = "FREE_RESPONSE"
_POLL         = "POLL"
_PSEUDO_NUM   = "PSEUDO_NUMERIC"
_NUMERIC      = "NUMERIC"


class ManifoldSource(Source):
    slug = "manifold"

    def extract(self, spark: SparkSession, cfg: SourceConfig) -> DataFrame:
        limit_markets     = int(cfg.params.get("limit_markets", 40))
        limit_bets        = int(cfg.params.get("limit_bets_per_market", 1000))
        min_bet_count     = int(cfg.params.get("min_bet_count", 5))
        sort              = str(cfg.params.get("sort", "updated-time"))
        include_resolved  = bool(cfg.params.get("include_resolved", False))
        bets_table        = cfg.params.get("bets_output")  # optional Delta table for raw bets

        markets = self._list_markets(limit_markets, sort, include_resolved, min_bet_count)
        _log.info("manifold_markets_listed", extra={"n": len(markets)})

        silver_rows: list[tuple] = []
        bet_rows:    list[tuple] = []

        source_pk = get_meta(self.slug).source_id
        cycle_dt = cfg.cycle_ts.date()

        for m in markets:
            try:
                self._enrich_and_emit(
                    m, cfg, limit_bets, silver_rows, bet_rows,
                    source_pk=source_pk, cycle_dt=cycle_dt,
                )
            except Exception as exc:  # noqa: BLE001 — one bad market shouldn't kill the run
                _log.warning("manifold_market_skipped", extra={
                    "market_id": m.get("id"), "slug": m.get("slug"), "err": repr(exc),
                })

        # Stash bets on self so the framework wrapper can write them to
        # the shared ``bets`` table. The wrapper (in
        # :mod:`wisdom_of_crowds.transformations.ingest`) reads
        # ``self._bets_df``; source callers that want the legacy in-source
        # write behaviour can still pass ``bets_output`` and this method
        # will honour it below.
        self._bets_df = (
            spark.createDataFrame(bet_rows, BETS_SCHEMA) if bet_rows
            else spark.createDataFrame([], BETS_SCHEMA)
        )
        if bet_rows and bets_table:
            # Legacy path: write directly. New (transformation-framework)
            # path leaves bets_table unset and lets the framework write.
            self._write_bets(
                self._bets_df, bets_table, source_pk=source_pk, cycle_dt=cycle_dt,
            )

        if not silver_rows:
            return spark.createDataFrame([], INGEST_SCHEMA)
        return spark.createDataFrame(silver_rows, INGEST_SCHEMA)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @staticmethod
    def _get_json(path: str, query: dict[str, Any] | None = None, retries: int = 3) -> Any:
        url = f"{_API_BASE}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        req = urllib.request.Request(url, headers={"User-Agent": "wisdom-of-crowds/0.2"})
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
        """Return up to ``limit`` markets, filtered client-side to eligible
        types and (optionally) unresolved."""
        pool: list[dict[str, Any]] = []
        # Manifold paginates via ``before`` = last id in the page.
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

    def _fetch_bets(self, contract_id: str, limit: int) -> list[dict[str, Any]]:
        return self._get_json("/bets", {"contractId": contract_id, "limit": limit})

    def _fetch_market_full(self, contract_id: str) -> dict[str, Any]:
        """Hydrate a single market via ``/market/{id}``.

        Manifold's ``/markets`` list endpoint returns "LiteMarket" objects
        that omit the ``answers`` array for MULTIPLE_CHOICE / FREE_RESPONSE
        markets — meaning the categorical strategy has no outcomes/prices
        to aggregate. Fetching the FullMarket for each such contract fills
        those in.
        """
        return self._get_json(f"/market/{contract_id}")

    # ------------------------------------------------------------------
    # per-market emission
    # ------------------------------------------------------------------

    def _enrich_and_emit(
        self,
        market: dict[str, Any],
        cfg: SourceConfig,
        limit_bets: int,
        silver_rows: list[tuple],
        bet_rows: list[tuple],
        *,
        source_pk: int,
        cycle_dt: dt.date,
    ) -> None:
        mid   = market["id"]
        slug  = market.get("slug")
        qtype = market["outcomeType"]

        # Fetch the individual bets (up to ``limit_bets``). These populate
        # the manifold_bets table AND may seed the numeric_guesses strategy
        # for PSEUDO_NUMERIC markets.
        bets = self._fetch_bets(mid, limit_bets)
        for b in bets:
            created = b.get("createdTime")
            created_ts = dt.datetime.fromtimestamp(created / 1000, tz=dt.timezone.utc) if created else None
            prob_after = _f(b.get("probAfter"))
            bet_rows.append((
                source_pk,
                self.slug,
                mid,
                slug,
                b.get("id"),
                b.get("userId"),
                _f(b.get("amount")),
                _f(b.get("shares")),
                b.get("outcome"),
                prob_after,                # price — Manifold's post-bet probability
                _f(b.get("probBefore")),
                prob_after,
                bool(b.get("isFilled")) if b.get("isFilled") is not None else None,
                created_ts,
                cfg.cycle_ts,
                cycle_dt,
            ))

        # Build the aggregate silver row per question type.
        question = market.get("question") or f"manifold:{mid}"
        end_ms   = market.get("closeTime") or market.get("resolutionTime")
        end_dt   = dt.datetime.fromtimestamp(end_ms / 1000, tz=dt.timezone.utc) if end_ms else None
        is_res   = bool(market.get("isResolved"))
        resolved_outcome = market.get("resolution") if is_res else None

        guesses      = None
        outcomes     = None
        prices       = None
        trader_count = int(market.get("uniqueBettorCount", 0)) or None
        volume       = _f(market.get("volume"))
        resolved_val = None

        if qtype == _BINARY:
            silver_qtype   = QuestionType.BINARY
            outcomes       = ["Yes", "No"]
            p_yes          = _f(market.get("probability"))
            prices         = [p_yes if p_yes is not None else 0.0,
                              1.0 - (p_yes if p_yes is not None else 0.0)]
        elif qtype == _POLL:
            silver_qtype   = QuestionType.POLL_CATEGORICAL
            opts           = market.get("options") or []
            total_votes    = sum(int(o.get("votes") or 0) for o in opts) or 1
            outcomes       = [str(o.get("text")) for o in opts]
            prices         = [float(int(o.get("votes") or 0)) / total_votes for o in opts]
            trader_count   = trader_count or total_votes
        elif qtype in (_MULTI, _FREE_RESP):
            silver_qtype   = QuestionType.CATEGORICAL
            ans            = market.get("answers") or []
            if not ans:
                # LiteMarket from /markets doesn't carry answers — hydrate
                # via /market/{id} so gold has real outcomes/prices.
                try:
                    full = self._fetch_market_full(mid)
                    ans = full.get("answers") or []
                except Exception as exc:  # noqa: BLE001
                    _log.warning("manifold_market_hydrate_failed", extra={
                        "market_id": mid, "err": repr(exc),
                    })
                    ans = []
            # Filter out already-resolved sub-answers. Manifold pins their
            # probability at 1.0 or 0.0 once resolved, which the categorical
            # strategy would otherwise report as the crowd's forecast — a
            # settled fact masquerading as a prediction. Answers with a
            # non-null ``resolution`` field are what we skip.
            ans = [a for a in ans if not a.get("resolution")]
            outcomes       = [str(a.get("text")) for a in ans]
            prices         = [_f(a.get("probability")) or 0.0 for a in ans]
        elif qtype == _PSEUDO_NUM:
            silver_qtype   = QuestionType.NUMERIC_GUESSES
            mn             = _f(market.get("min"))
            mx             = _f(market.get("max"))
            log_scale      = bool(market.get("isLogScale"))
            # Convert each bet's probAfter into a numeric guess in [min, max].
            probs = [_f(b.get("probAfter")) for b in bets if b.get("probAfter") is not None]
            if mn is not None and mx is not None and probs:
                guesses = [_map_prob_to_value(p, mn, mx, log_scale) for p in probs]
            else:
                guesses = []
        else:
            return  # unsupported type

        silver_rows.append((
            self.slug,
            mid,
            question,
            silver_qtype,
            guesses,
            outcomes,
            prices,
            trader_count,
            volume,
            end_dt,
            is_res,
            resolved_outcome,
            resolved_val,
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
        """Write the raw-bets table, partitioned by ``(cycle_dt, source_id)``.

        The ``replaceWhere`` clause scopes the overwrite to *this* run's
        (source_id, cycle_dt) partition only. Running the same source twice
        in one day is idempotent: the second run truncate-loads the same
        partition. A different source running the same day, or the same
        source running a different day, is untouched.
        """
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
        _log.info("manifold_bets_written", extra={"target": target, "rows": df.count()})


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _f(x: Any) -> float | None:
    """Best-effort float cast; returns None for null/nan/non-numeric."""
    if x is None:
        return None
    try:
        v = float(x)
        return v if v == v else None  # filter NaN
    except (TypeError, ValueError):
        return None


def _map_prob_to_value(p: float, mn: float, mx: float, log_scale: bool) -> float:
    if log_scale:
        # Manifold's convention: value = (max - min + 1)^p + min - 1
        return (mx - mn + 1.0) ** p + mn - 1.0
    return mn + (mx - mn) * p


def _bets_from_market_rows(rows: Iterable[tuple]) -> ArrayType:  # noqa: ARG001 — reserved
    """(Unused; placeholder for future in-DF bet aggregation.)"""
    return ArrayType(DoubleType())
