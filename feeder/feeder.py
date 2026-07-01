"""TCP feeder: replays the pre-sorted events.csv as a stream over a socket.

The CSV is assumed already ordered by event_time (see src/preprocess.py).
Inter-event delays are derived from the event_time column; we sleep
(delta_event_time / acceleration) between two consecutive emissions.

The feeder acts as a TCP server: Flink's socket_text_stream is the client
and connects to feeder:9999 inside the docker network.

Usage:
    python feeder.py --input /opt/feeder/data/events.csv \\
                     --port 9999 \\
                     --acceleration 3600
"""
import argparse
import csv
import socket
import sys
import time


def serve(input_path: str, port: int, acceleration: float):
    print(f"[feeder] loading sorted CSV from {input_path}", flush=True)
    with open(input_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        # locate event_time column (last column by convention)
        et_idx = header.index("event_time")
        rows = list(reader)
    print(f"[feeder] loaded {len(rows):,} rows. acceleration = {acceleration}",
          flush=True)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(1)
    print(f"[feeder] listening on 0.0.0.0:{port}", flush=True)

    while True:
        conn, addr = srv.accept()
        print(f"[feeder] client connected: {addr}", flush=True)
        wall_start = time.time()
        first_et = int(rows[0][et_idx])
        sent = 0
        try:
            for i, row in enumerate(rows):
                et = int(row[et_idx])
                # target wall-clock relative to first event
                target = (et - first_et) / 1000.0 / acceleration
                elapsed = time.time() - wall_start
                if target > elapsed:
                    time.sleep(target - elapsed)
                line = ",".join(row) + "\n"
                conn.sendall(line.encode("utf-8"))
                sent += 1
                if sent % 50_000 == 0:
                    print(f"[feeder] sent {sent:,} / {len(rows):,} "
                          f"(elapsed {elapsed:.1f}s)", flush=True)
            print(f"[feeder] done: sent {sent:,} rows", flush=True)
        except (BrokenPipeError, ConnectionResetError):
            print(f"[feeder] client disconnected after {sent:,} rows",
                  flush=True)
        finally:
            conn.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="/opt/feeder/data/events.csv")
    p.add_argument("--port", type=int, default=9999)
    p.add_argument("--acceleration", type=float, default=3600.0,
                   help="event-time speedup factor (e.g. 3600 => 1h event = 1s real)")
    args = p.parse_args()
    try:
        serve(args.input, args.port, args.acceleration)
    except KeyboardInterrupt:
        print("[feeder] shutting down", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
