"""Build the final Q2 CSVs from:
  * data/events.csv  (source-of-truth event stream, sorted by event time)
  * Results/q2_1h/*.csv  (per-airport counts/sums/max for each 1h window,
                          produced by the Flink job query2.py)
  * Results/q2_6h/*.csv  (idem, but 6h windows)

For each of the three required windows (1h, 6h, full dataset) we:
  1. Group rows by (window_start, window_end).
  2. For each window, take airports with >= 30 non-cancelled non-diverted
     flights and rank by descending severe_delays count.
  3. Take the top-10 airports.
  4. For each top-10 airport, scan events.csv to recover the actual list
     of severely delayed flights (DEP_DELAY > 30, not cancelled, not
     diverted) and keep the top-20 by DEP_DELAY descending.
  5. Emit the final CSV with the schema required by the project spec.

Output files (written next to Results/):
    Results/q2_1h_final.csv
    Results/q2_6h_final.csv
    Results/q2_global_final.csv

The 'global' window is computed by re-aggregating the 1h pre-aggregates
plus a single events.csv scan for the top-20 list.

This script depends only on the Python standard library (no pandas
needed) so it can run on any host without extra setup.
"""
import argparse
import csv
import os
from collections import defaultdict
from glob import glob

MIN_FLIGHTS_PER_AIRPORT = 30
TOP_K = 10
MAX_DELAYED_LIST = 20
SEVERE_DELAY_THRESHOLD = 30.0


# ----- helpers --------------------------------------------------------------

def _read_flink_outputs(in_dir):
    """Yield (window_start, window_end, airport_id, num, severe, sum_d, max_d)."""
    csvs = sorted(glob(os.path.join(in_dir, "**", "*.csv"), recursive=True))
    csvs = [p for p in csvs if ".inprogress" not in p]
    n = 0
    for path in csvs:
        with open(path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split(",")
                if len(parts) != 7:
                    continue
                ws, we, aid, num, severe, sum_d, max_d = parts
                yield (ws, we, int(aid), int(num), int(severe),
                       float(sum_d), float(max_d))
                n += 1
    print(f"[postprocess] {in_dir}: {n} per-airport rows from {len(csvs)} parts")


def _build_window_groups(in_dir):
    """Return dict: (ws, we) -> list of dicts per airport."""
    groups = defaultdict(list)
    for ws, we, aid, num, severe, sum_d, max_d in _read_flink_outputs(in_dir):
        groups[(ws, we)].append({
            "airport_id": aid,
            "num": num,
            "severe": severe,
            "sum_delay": sum_d,
            "max_delay": max_d,
        })
    return groups


def _events_iter(events_csv):
    """Yield (event_time_ms, airport_id, carrier, dest, dep_delay) for
    every non-cancelled non-diverted flight in events.csv. Cheap: streamed."""
    with open(events_csv, "r") as f:
        r = csv.reader(f)
        header = next(r)
        col = {name: i for i, name in enumerate(header)}
        for row in r:
            try:
                cancelled = float(row[col["CANCELLED"]] or 0)
                diverted = float(row[col["DIVERTED"]] or 0)
                if cancelled >= 1.0 or diverted >= 1.0:
                    continue
                yield (
                    int(row[col["event_time"]]),
                    int(row[col["ORIGIN_AIRPORT_ID"]]),
                    row[col["OP_UNIQUE_CARRIER"]],
                    int(row[col["DEST_AIRPORT_ID"]]),
                    float(row[col["DEP_DELAY"]] or 0.0),
                )
            except (KeyError, ValueError, IndexError):
                continue


def _ts_to_ms(ts_str):
    """'2025-01-01 08:00:00' -> ms epoch (UTC)."""
    import datetime as _dt
    dt = _dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)


def _collect_top20_for_winners(events_csv, winners_per_window):
    """For each (window, airport) listed in winners_per_window, scan
    events.csv and collect the top-20 severely delayed flights.

    winners_per_window: dict (ws_ms, we_ms) -> set(airport_id)
    Returns: dict (ws_ms, we_ms, airport_id) -> list[(carrier, dest, delay)]

    Single sequential scan of events.csv keeps this O(N).
    """
    # Build a quick lookup keyed by airport_id, then per-airport windows sorted.
    # For O(1) per event: per airport, store sorted (ws_ms, we_ms) intervals.
    airport_windows = defaultdict(list)
    for (ws_ms, we_ms), airports in winners_per_window.items():
        for aid in airports:
            airport_windows[aid].append((ws_ms, we_ms))
    for aid in airport_windows:
        airport_windows[aid].sort()

    # Result store: list of (delay, carrier, dest) per (ws, we, aid)
    delayed = defaultdict(list)

    n_scanned = 0
    n_severe = 0
    for et_ms, aid, carrier, dest, dep_delay in _events_iter(events_csv):
        n_scanned += 1
        if dep_delay <= SEVERE_DELAY_THRESHOLD:
            continue
        wins = airport_windows.get(aid)
        if not wins:
            continue
        n_severe += 1
        # For each window of this airport that covers et_ms, append.
        for ws_ms, we_ms in wins:
            if ws_ms <= et_ms < we_ms:
                delayed[(ws_ms, we_ms, aid)].append((dep_delay, carrier, dest))
            elif et_ms < ws_ms:
                break  # since wins is sorted

    print(f"[postprocess]   scanned {n_scanned:,} flights, "
          f"{n_severe:,} severe matched winners")

    # Trim each list to top-20 by delay desc.
    final = {}
    for k, lst in delayed.items():
        lst.sort(reverse=True)
        final[k] = [(c, d, de) for (de, c, d) in lst[:MAX_DELAYED_LIST]]
    return final


# ----- final emission -------------------------------------------------------

def _emit_final(out_path, windows, winners_top20):
    """windows: dict (ws_str, we_str) -> sorted list of dicts (top-10 airports
        already picked); winners_top20: dict (ws_ms, we_ms, aid) -> list."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow([
            "ts", "rank", "origin_airport_id",
            "num_flights", "severe_delays",
            "dep_delay_mean", "dep_delay_max", "delayed_flights",
        ])
        for (ws_str, we_str), ranked in windows.items():
            ws_ms = _ts_to_ms(ws_str)
            we_ms = _ts_to_ms(we_str)
            for rank, r in enumerate(ranked, 1):
                top = winners_top20.get((ws_ms, we_ms, r["airport_id"]), [])
                delayed_repr = "[" + ", ".join(
                    f"({c},{d},{de:.2f})" for c, d, de in top
                ) + "]"
                mean_delay = r["sum_delay"] / r["num"] if r["num"] else 0.0
                w.writerow([
                    ws_str, rank, r["airport_id"],
                    r["num"], r["severe"],
                    f"{mean_delay:.2f}", f"{r['max_delay']:.2f}",
                    delayed_repr,
                ])
    print(f"[postprocess]   wrote {out_path}")


def _rank_window(rows):
    valid = [r for r in rows if r["num"] >= MIN_FLIGHTS_PER_AIRPORT]
    valid.sort(key=lambda r: (-r["severe"], -r["num"]))
    return valid[:TOP_K]


# ----- per-window orchestration --------------------------------------------

def process_window(in_dir, events_csv, out_path):
    print(f"[postprocess] === {in_dir} -> {out_path} ===")
    groups = _build_window_groups(in_dir)
    if not groups:
        print(f"[postprocess]   skipping: no Flink output found in {in_dir}")
        return

    # Pick top-10 per window first.
    windows_sorted = {}
    winners_per_window_ms = {}
    for (ws_str, we_str), rows in sorted(groups.items()):
        ranked = _rank_window(rows)
        if not ranked:
            continue
        windows_sorted[(ws_str, we_str)] = ranked
        ws_ms = _ts_to_ms(ws_str)
        we_ms = _ts_to_ms(we_str)
        winners_per_window_ms[(ws_ms, we_ms)] = {r["airport_id"] for r in ranked}

    print(f"[postprocess]   {len(windows_sorted)} windows have a ranking")

    # Scan events.csv to harvest top-20 lists for every winner.
    top20 = _collect_top20_for_winners(events_csv, winners_per_window_ms)
    _emit_final(out_path, windows_sorted, top20)


def process_global(events_csv, q2_1h_dir, out_path):
    """Re-aggregate 1h pre-aggregates by airport over the whole dataset,
    then build the single 'global' ranking + scan events.csv for top-20."""
    print(f"[postprocess] === GLOBAL (from {q2_1h_dir}) -> {out_path} ===")
    by_airport = {}
    earliest_ws = None
    latest_we = None
    for ws_str, we_str, aid, num, severe, sum_d, max_d in _read_flink_outputs(q2_1h_dir):
        s = by_airport.get(aid)
        if s is None:
            s = {"airport_id": aid, "num": 0, "severe": 0,
                 "sum_delay": 0.0, "max_delay": float("-inf")}
            by_airport[aid] = s
        s["num"] += num
        s["severe"] += severe
        s["sum_delay"] += sum_d
        if max_d > s["max_delay"]:
            s["max_delay"] = max_d
        if earliest_ws is None or ws_str < earliest_ws:
            earliest_ws = ws_str
        if latest_we is None or we_str > latest_we:
            latest_we = we_str

    if not by_airport:
        print(f"[postprocess]   skipping global: no Flink 1h output found")
        return

    ranked = _rank_window(list(by_airport.values()))
    if not ranked:
        print(f"[postprocess]   skipping global: no airport meets >= 30 flights")
        return

    print(f"[postprocess]   global window {earliest_ws} -> {latest_we}, "
          f"{len(ranked)} winners")

    # Build winners_per_window keyed by global window for the top-20 scan.
    ws_ms = _ts_to_ms(earliest_ws)
    we_ms = _ts_to_ms(latest_we)
    winners = {(ws_ms, we_ms): {r["airport_id"] for r in ranked}}
    top20 = _collect_top20_for_winners(events_csv, winners)

    windows = {(earliest_ws, latest_we): ranked}
    _emit_final(out_path, windows, top20)


# ----- main -----------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-root", default="Results")
    ap.add_argument("--events-csv", default="data/events.csv")
    args = ap.parse_args()

    process_window(
        os.path.join(args.results_root, "q2_1h"),
        args.events_csv,
        os.path.join(args.results_root, "q2_1h_final.csv"),
    )
    process_window(
        os.path.join(args.results_root, "q2_6h"),
        args.events_csv,
        os.path.join(args.results_root, "q2_6h_final.csv"),
    )
    process_global(
        args.events_csv,
        os.path.join(args.results_root, "q2_1h"),
        os.path.join(args.results_root, "q2_global_final.csv"),
    )


if __name__ == "__main__":
    main()
