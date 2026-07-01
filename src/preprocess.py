"""Combine the 4 monthly CSVs into a single events.csv sorted by event time.

Event time is derived from YEAR + MONTH + DAY_OF_MONTH + CRS_DEP_TIME.
This script runs once before the Flink job; Flink then consumes the
sorted file as its source stream.
"""
import argparse
import os
import sys

import pandas as pd


RELEVANT_COLS = [
    "YEAR", "MONTH", "DAY_OF_MONTH",
    "OP_UNIQUE_CARRIER",
    "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
    "DEP_DELAY", "ARR_DELAY",
    "CANCELLED", "DIVERTED",
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
]


def build_event_time(df: pd.DataFrame) -> pd.Series:
    """Compose a Unix-ms timestamp from YEAR/MONTH/DAY/CRS_DEP_TIME."""
    hh = (df["CRS_DEP_TIME"].fillna(0).astype(int) // 100).clip(0, 23)
    mm = (df["CRS_DEP_TIME"].fillna(0).astype(int) % 100).clip(0, 59)
    ts = pd.to_datetime({
        "year": df["YEAR"], "month": df["MONTH"], "day": df["DAY_OF_MONTH"],
        "hour": hh, "minute": mm,
    })
    return (ts.astype("int64") // 1_000_000)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data", help="Directory with monthly CSVs")
    parser.add_argument("--output", default="data/events.csv", help="Output sorted file")
    args = parser.parse_args()

    csvs = sorted(f for f in os.listdir(args.input)
                  if f.endswith(".csv") and f.startswith("2025"))
    if not csvs:
        print(f"No monthly CSVs found in {args.input}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {len(csvs)} monthly files...")
    dfs = []
    for f in csvs:
        path = os.path.join(args.input, f)
        df = pd.read_csv(path, usecols=lambda c: c in RELEVANT_COLS)
        print(f"  {f}: {len(df):,} rows")
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    print(f"Total rows: {len(df):,}")

    print("Computing event_time and sorting...")
    df["event_time"] = build_event_time(df)
    df = df.sort_values("event_time", kind="mergesort").reset_index(drop=True)

    print(f"Writing {args.output} ...")
    df.to_csv(args.output, index=False)
    print(f"Done. First event: {df.iloc[0]['event_time']}, "
          f"last event: {df.iloc[-1]['event_time']}")


if __name__ == "__main__":
    main()
