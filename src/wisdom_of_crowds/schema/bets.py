"""``bets`` — one row per individual bet or trade.

The generic per-individual table. Any source that publishes each
participant's own bet (Manifold today, Polymarket next, Kalshi if we add
it later) writes into this one table, partitioned by
``(cycle_dt, source_id)`` — so parallel writes from different sources
land in disjoint partitions and never collide.

Columns marked "nullable" are populated by some sources and left blank by
others. That keeps the table generic without forcing every source to
invent values it doesn't have. The core (``source``, ``market_id``,
``bet_id``, ``created_at``, ``price``, ``outcome``) is populated by all.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class BetColumns:
    SOURCE_ID    = "source_id"     # INT FK into wisdom_of_crowds.core.sources
    SOURCE       = "source"        # slug, e.g. "manifold" | "polymarket"
    MARKET_ID    = "market_id"     # source's own market ID
    MARKET_SLUG  = "market_slug"   # source's URL slug (optional)
    BET_ID       = "bet_id"        # unique per source (bet ID / trade hash)
    USER_ID      = "user_id"       # anonymised trader ID (nullable)
    AMOUNT       = "amount"        # native size (mana / USD / contracts)
    SHARES       = "shares"        # shares acquired (Manifold-style, nullable)
    OUTCOME      = "outcome"       # YES / NO / answer text
    PRICE        = "price"         # normalised probability (0..1), fills prob_after for Manifold
    PROB_BEFORE  = "prob_before"   # market probability before the bet (nullable)
    PROB_AFTER   = "prob_after"    # market probability after the bet (nullable; = price when set)
    IS_FILLED    = "is_filled"     # limit-order fill flag (nullable)
    CREATED_AT   = "created_at"    # when the bet happened
    CYCLE_TS     = "cycle_ts"      # ingest run timestamp
    CYCLE_DT     = "cycle_dt"      # ingest date, partition key


BETS_SCHEMA: StructType = StructType([
    StructField(BetColumns.SOURCE_ID,   IntegerType(),   nullable=False),
    StructField(BetColumns.SOURCE,      StringType(),    nullable=False),
    StructField(BetColumns.MARKET_ID,   StringType(),    nullable=False),
    StructField(BetColumns.MARKET_SLUG, StringType(),    nullable=True),
    StructField(BetColumns.BET_ID,      StringType(),    nullable=False),
    StructField(BetColumns.USER_ID,     StringType(),    nullable=True),
    StructField(BetColumns.AMOUNT,      DoubleType(),    nullable=True),
    StructField(BetColumns.SHARES,      DoubleType(),    nullable=True),
    StructField(BetColumns.OUTCOME,     StringType(),    nullable=True),
    StructField(BetColumns.PRICE,       DoubleType(),    nullable=True),
    StructField(BetColumns.PROB_BEFORE, DoubleType(),    nullable=True),
    StructField(BetColumns.PROB_AFTER,  DoubleType(),    nullable=True),
    StructField(BetColumns.IS_FILLED,   BooleanType(),   nullable=True),
    StructField(BetColumns.CREATED_AT,  TimestampType(), nullable=True),
    StructField(BetColumns.CYCLE_TS,    TimestampType(), nullable=False),
    StructField(BetColumns.CYCLE_DT,    DateType(),      nullable=False),
])
"""Schema for the ``wisdom_of_crowds.core.bets`` table."""
