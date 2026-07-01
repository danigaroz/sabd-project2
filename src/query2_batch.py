"""SABD Project 2 — Query 2 (batch implementation).

This is a pragmatic fallback: Q2 implemented as a pandas batch job over
the pre-sorted events.csv that the feeder uses to drive Q1.

Why a batch fallback (motivated in the report):
    - Q1 already demonstrates command of the Flink DataStream API
      (event-time windows, watermarks, keyed aggregation, sinks).
    - Q2 in PyFlink iterations hit hard throughput ceilings: with
      ~360 keyed states across 2-3 windows, the Java<->Python RPC of
      Beam-based PyFlink AggregateFunctions cannot sustain the full
      replay before the TaskManager exhausts memory.
    - The Project 2 spec explicitly allows 1-student teams to
      "simulare lo stream di input e scrivere i risultati in formato
      CSV". The feeder simulates the stream; this batch processor
      preserves the event-time semantics (windows are computed over
      the event_time field, not wall-clock) and emits CSVs matching
      the spec schema exactly.

Inputs:
    data/events.csv  (the pre-sorted event stream; same one the feeder uses)

Outputs:
    Results/q2_1h_final.csv
    Results/q2_6h_final.csv
    Results/q2_global_final.csv

CSV schema (per spec):
    ts, rank, origin_airport_id, num_flights, severe_delays,
    dep_delay_mean, dep_delay_max, delayed_flights
"""
import argparse
import os
import time
from datetime import datetime, timezone

import pandas as pd

MIN_FLIGHTS_PER_AIRPORT = 30
TOP_K = 10
MAX_DELAYED_LIST = 20
SEVERE_DELAY_THRESHOLD = 30.0


def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) \
        .strftime("%Y-%m-%d %H:%M:%S")


def _build_delayed_repr(top: pd.DataFrame) -> str:
    """top: DataFrame with columns OP_UNIQUE_CARRIER, DEST_AIRPORT_ID, DEP_DELAY,
    already sorted descending by DEP_DELAY and truncated to MAX_DELAYED_LIST."""
    pieces = [
        f"({r.OP_UNIQUE_CARRIER},{r.DEST_AIRPORT_ID},{r.DEP_DELAY:.2f})"
        for r in top.itertuples(index=False)
    ]
    return "[" + ", ".join(pieces) + "]"


def _emit_for_window(df: pd.DataFrame, ws_ms_col: str, we_ms_col: str,
                     out_path: str):
    """For each (ws, we) bucket in df, pick top-10 airports and emit final CSV.
    df must already have per-(window, airport) stats in columns:
      num, severe, sum_delay, max_delay
    plus airport_id and ws_ms / we_ms.
    df also carries a 'raw_severe' DataFrame attached per group via a
    pre-computed map; here we receive it indirectly through df_full.
    """
    raise NotImplementedError("use _process_window instead")


def _process_window(df_events: pd.DataFrame, freq: str, out_path: str,
                    label: str):
    """Compute Q2 for one window size and write the CSV.

    df_events: events as DataFrame, already filtered to non-cancelled
        non-diverted flights, with event_time and bucket columns.
    freq: pandas frequency string ('1h', '6h') or None for 'global'.
    """
    t0 = time.time()
    print(f"\n[Q2-batch] === {label} window -> {out_path} ===")

    if freq is None:
        # Single global window spanning the whole dataset.
        df_events = df_events.copy()
        df_events["bucket_start_ms"] = df_events["event_time"].min()
        df_events["bucket_end_ms"]   = df_events["event_time"].max() + 1
    else:
        # Tumbling bucket aligned to UTC epoch:
        floor_freq = freq
        dt = pd.to_datetime(df_events["event_time"], unit="ms", utc=True)
        bucket_start = dt.dt.floor(floor_freq)
        # we_ms = ws_ms + step
        step_ms = {"1h": 3600 * 1000, "6h": 6 * 3600 * 1000}[freq]
        ws_ms = (bucket_start.astype("int64") // 1_000_000)
        df_events = df_events.assign(
            bucket_start_ms=ws_ms.values,
            bucket_end_ms=(ws_ms + step_ms).values,
        )

    # Stats per (window, airport)
    grp = df_events.groupby(
        ["bucket_start_ms", "bucket_end_ms", "ORIGIN_AIRPORT_ID"],
        sort=False,
    )
    stats = grp.agg(
        num=("DEP_DELAY", "size"),
        severe=("DEP_DELAY", lambda s: int((s > SEVERE_DELAY_THRESHOLD).sum())),
        sum_delay=("DEP_DELAY", "sum"),
        max_delay=("DEP_DELAY", "max"),
    ).reset_index()

    # Filter ">= 30 non-cancelled non-diverted flights per airport"
    stats = stats[stats["num"] >= MIN_FLIGHTS_PER_AIRPORT]
    print(f"[Q2-batch]   {len(stats):,} (window, airport) rows pass >=30 filter")

    # Rank per window: severe desc, num desc tiebreak.
    stats["__rank_key"] = list(zip(-stats["severe"], -stats["num"]))
    stats = stats.sort_values(
        ["bucket_start_ms", "severe", "num"],
        ascending=[True, False, False],
        kind="mergesort",
    )
    stats["rank"] = stats.groupby("bucket_start_ms").cumcount() + 1
    top = stats[stats["rank"] <= TOP_K].copy()
    print(f"[Q2-batch]   {len(top):,} top-{TOP_K} rows across "
          f"{top['bucket_start_ms'].nunique()} windows")

    # Get severely-delayed flights per (window, airport) just for the winners.
    winners_set = set(
        (int(ws), int(aid))
        for ws, aid in zip(top["bucket_start_ms"], top["ORIGIN_AIRPORT_ID"])
    )

    severe_flights = df_events[df_events["DEP_DELAY"] > SEVERE_DELAY_THRESHOLD]
    severe_flights = severe_flights[[
        "bucket_start_ms", "ORIGIN_AIRPORT_ID",
        "OP_UNIQUE_CARRIER", "DEST_AIRPORT_ID", "DEP_DELAY",
    ]]
    # Sort once to be able to truncate per-group cheaply.
    severe_flights = severe_flights.sort_values(
        ["bucket_start_ms", "ORIGIN_AIRPORT_ID", "DEP_DELAY"],
        ascending=[True, True, False],
        kind="mergesort",
    )
    # Keep only winners.
    sf = severe_flights[severe_flights.apply(
        lambda r: (int(r["bucket_start_ms"]), int(r["ORIGIN_AIRPORT_ID"])) in winners_set,
        axis=1,
    )]
    # head(MAX_DELAYED_LIST) per (window, airport)
    top20_per_pair = (
        sf.groupby(["bucket_start_ms", "ORIGIN_AIRPORT_ID"], sort=False)
          .head(MAX_DELAYED_LIST)
    )

    # Build a dict (ws, aid) -> dataframe of top-20 severe flights
    delayed_map = {}
    for (ws, aid), sub in top20_per_pair.groupby(
        ["bucket_start_ms", "ORIGIN_AIRPORT_ID"], sort=False,
    ):
        delayed_map[(int(ws), int(aid))] = sub

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        f.write("ts,rank,origin_airport_id,num_flights,severe_delays,"
                "dep_delay_mean,dep_delay_max,delayed_flights\n")
        for r in top.itertuples(index=False):
            ws_str = _fmt_ts(int(r.bucket_start_ms))
            aid = int(r.ORIGIN_AIRPORT_ID)
            mean_delay = r.sum_delay / r.num if r.num else 0.0
            sub = delayed_map.get((int(r.bucket_start_ms), aid))
            if sub is None or sub.empty:
                delayed_repr = "[]"
            else:
                delayed_repr = _build_delayed_repr(sub)
            # CSV quote the delayed list because it has commas
            f.write(
                f'{ws_str},{int(r.rank)},{aid},{int(r.num)},{int(r.severe)},'
                f'{mean_delay:.2f},{r.max_delay:.2f},"{delayed_repr}"\n'
            )
    elapsed = time.time() - t0
    print(f"[Q2-batch]   wrote {out_path} in {elapsed:.1f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events-csv", default="data/events.csv")
    ap.add_argument("--results-root", default="Results")
    args = ap.parse_args()

    print(f"[Q2-batch] reading {args.events_csv} ...")
    t0 = time.time()
    df = pd.read_csv(
        args.events_csv,
        usecols=[
            "OP_UNIQUE_CARRIER",
            "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
            "DEP_DELAY", "CANCELLED", "DIVERTED",
            "event_time",
        ],
        dtype={
            "OP_UNIQUE_CARRIER": "string",
            "ORIGIN_AIRPORT_ID": "Int32",
            "DEST_AIRPORT_ID": "Int32",
            "DEP_DELAY": "float32",
            "CANCELLED": "float32",
            "DIVERTED": "float32",
            "event_time": "int64",
        },
    )
    print(f"[Q2-batch]   {len(df):,} rows in {time.time()-t0:.1f}s")

    # Spec: only non-cancelled, non-diverted
    df = df[(df["CANCELLED"] < 1.0) & (df["DIVERTED"] < 1.0)].copy()
    df["DEP_DELAY"] = df["DEP_DELAY"].fillna(0.0)
    print(f"[Q2-batch]   {len(df):,} rows after cancel/divert filter")

    _process_window(df, "1h",  os.path.join(args.results_root, "q2_1h_final.csv"),     "1h")
    _process_window(df, "6h",  os.path.join(args.results_root, "q2_6h_final.csv"),     "6h")
    _process_window(df, None,  os.path.join(args.results_root, "q2_global_final.csv"), "global")
    print("\n[Q2-batch] done.")


if __name__ == "__main__":
    main()
