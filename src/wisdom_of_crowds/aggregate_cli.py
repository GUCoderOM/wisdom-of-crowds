"""CLI entrypoint for the aggregate job. Wraps :mod:`.aggregate.run`."""

from __future__ import annotations

import argparse
import datetime as dt
import logging

from pyspark.sql import SparkSession

from wisdom_of_crowds.aggregate import AggregateJobConfig, run


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Wisdom-of-crowds aggregate job.")
    p.add_argument("--guesses-source", required=True,
                   help="Silver Delta table name or Parquet directory to read from.")
    p.add_argument("--wisdom-output",   required=True,
                   help="Gold Delta table name or Parquet directory to write to.")
    p.add_argument("--cycle-dt",      default=None,
                   help="ISO date for this cycle (YYYY-MM-DD). Default: today (UTC).")
    p.add_argument("--source-slug",   default=None,
                   help="If set, aggregate only this source's (cycle_dt, source_id) partition.")
    return p


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args(argv)
    cycle_dt = (
        dt.date.fromisoformat(args.cycle_dt)
        if args.cycle_dt else dt.datetime.now(dt.timezone.utc).date()
    )
    source_id: int | None = None
    if args.source_slug:
        from wisdom_of_crowds.schema.sources import get_meta
        source_id = get_meta(args.source_slug).source_id
    spark = SparkSession.builder.appName("vox-aggregate").getOrCreate()
    run(spark, AggregateJobConfig(
        guesses_source=args.guesses_source,
        wisdom_output=args.wisdom_output,
        cycle_dt=cycle_dt,
        source_id=source_id,
    ))


if __name__ == "__main__":  # pragma: no cover
    main()
