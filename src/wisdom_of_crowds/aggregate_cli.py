"""CLI entrypoint for the aggregate job. Wraps :mod:`.aggregate.run`."""

from __future__ import annotations

import argparse
import datetime as dt
import logging

from pyspark.sql import SparkSession

from wisdom_of_crowds.aggregate import AggregateJobConfig, run


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Wisdom-of-crowds aggregate job.")
    p.add_argument("--silver-source", required=True,
                   help="Silver Delta table name or Parquet directory to read from.")
    p.add_argument("--gold-output",   required=True,
                   help="Gold Delta table name or Parquet directory to write to.")
    p.add_argument("--cycle-dt",      default=None,
                   help="ISO date for this cycle (YYYY-MM-DD). Default: today (UTC).")
    return p


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args(argv)
    cycle_dt = (
        dt.date.fromisoformat(args.cycle_dt)
        if args.cycle_dt else dt.datetime.now(dt.timezone.utc).date()
    )
    spark = SparkSession.builder.appName("vox-aggregate").getOrCreate()
    run(spark, AggregateJobConfig(
        silver_source=args.silver_source,
        gold_output=args.gold_output,
        cycle_dt=cycle_dt,
    ))


if __name__ == "__main__":  # pragma: no cover
    main()
