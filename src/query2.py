"""SABD Project 2 — Query 2 (PyFlink).

PRE-AGGREGATION STAGE.

For each origin airport and each event-time window we emit a compact
record with the per-airport statistics needed for Q2:

    window_start, window_end, airport_id,
    num_flights, severe_delays, sum_dep_delay, max_dep_delay,
    delayed_flights_top20

The cross-airport TOP-10 ranking, filtering on ">= 30 non-cancelled
non-diverted flights" and the final CSV format are computed by the
post-processing script `src/postprocess_q2.py` from these per-airport
records. This split lets Flink stay incremental and bounded in memory
(O(airports) state per window instead of O(events)).

Three windows are produced:
    * 1 hour     -> Results/q2_1h
    * 6 hours    -> Results/q2_6h
    * 120 days   -> Results/q2_global (approximates 'from start of dataset')

Filter: only non-cancelled, non-diverted flights enter the aggregator
(per spec wording 'considerando solo i voli non cancellati e non deviati').
"""
import argparse
import os
from datetime import datetime

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import Encoder
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.connectors.file_system import (
    FileSink, OutputFileConfig, RollingPolicy,
)
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

SEVERE_DELAY_THRESHOLD = 30.0
MAX_DELAYED_LIST = 20


# ----- parser ---------------------------------------------------------------

def parse_event(line: str):
    """Parse one CSV line; yield only non-cancelled, non-diverted flights."""
    try:
        p = line.strip().split(",")
        if len(p) < 17:
            return
        def _f(s):
            return float(s) if s.strip() != "" else 0.0
        cancelled = _f(p[9])
        diverted = _f(p[10])
        if cancelled >= 1.0 or diverted >= 1.0:
            return
        yield (
            int(p[4]),           # ORIGIN_AIRPORT_ID
            p[3],                # OP_UNIQUE_CARRIER
            int(p[5]),           # DEST_AIRPORT_ID
            _f(p[7]),            # DEP_DELAY
            int(p[16]),          # event_time (ms)
        )
    except Exception:
        return


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[4]


# ----- per-airport incremental aggregator -----------------------------------

class AirportAggregator(AggregateFunction):
    """Maintain compact per-airport stats.

    Accumulator layout (tuple):
      (num, severe, sum_delay, max_delay, top20_delayed)
    where top20_delayed is a python list of tuples
    [(carrier, dest, dep_delay), ...] kept bounded to MAX_DELAYED_LIST
    by sorting and trimming on each add.
    """

    def create_accumulator(self):
        return (0, 0, 0.0, float("-inf"), [])

    def add(self, value, acc):
        airport_id, carrier, dest, dep_delay, _ = value
        num, severe, sum_d, max_d, top = acc
        num += 1
        sum_d += dep_delay
        if dep_delay > max_d:
            max_d = dep_delay
        if dep_delay > SEVERE_DELAY_THRESHOLD:
            severe += 1
            top.append((carrier, dest, dep_delay))
            # Trim to keep memory bounded.
            if len(top) > MAX_DELAYED_LIST * 4:
                top.sort(key=lambda x: -x[2])
                del top[MAX_DELAYED_LIST:]
        return (num, severe, sum_d, max_d, top)

    def get_result(self, acc):
        # Final sort+trim of the delayed list for emission.
        num, severe, sum_d, max_d, top = acc
        top.sort(key=lambda x: -x[2])
        return (num, severe, sum_d, max_d, top[:MAX_DELAYED_LIST])

    def merge(self, a, b):
        num = a[0] + b[0]
        severe = a[1] + b[1]
        sum_d = a[2] + b[2]
        max_d = max(a[3], b[3])
        merged = list(a[4]) + list(b[4])
        merged.sort(key=lambda x: -x[2])
        return (num, severe, sum_d, max_d, merged[:MAX_DELAYED_LIST])


# ----- window function: format one CSV line per (window, airport) ----------

class FormatAirportFn(ProcessWindowFunction):
    def process(self, key, ctx, elements):
        acc = next(iter(elements))
        num, severe, sum_d, max_d, top = acc
        if num == 0:
            return
        ws = ctx.window().start
        we = ctx.window().end
        def fmt(ms):
            return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        # top encoded as 'carrier|dest|delay;carrier|dest|delay;...'
        # using pipes and semicolons to avoid CSV-comma collisions.
        top_str = ";".join(f"{c}|{d}|{de:.2f}" for c, d, de in top)
        airport_id = key
        yield (f"{fmt(ws)},{fmt(we)},{airport_id},"
               f"{num},{severe},{sum_d:.2f},{max_d:.2f},{top_str}")


# ----- sink helper ----------------------------------------------------------

def make_sink(directory: str, prefix: str) -> FileSink:
    os.makedirs(directory, exist_ok=True)
    return (
        FileSink
        .for_row_format(directory, Encoder.simple_string_encoder())
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix(prefix)
            .with_part_suffix(".csv")
            .build()
        )
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )


# ----- main -----------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="feeder")
    ap.add_argument("--port", type=int, default=9999)
    ap.add_argument("--output-root", default="/opt/flink/app/Results")
    ap.add_argument("--parallelism", type=int, default=1)
    ap.add_argument("--watermark-lateness-sec", type=int, default=60)
    args = ap.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    # Source via Java env (PyFlink 1.20 doesn't expose socket_text_stream).
    j_socket = env._j_stream_execution_environment.socketTextStream(
        args.host, args.port
    )
    raw = DataStream(j_socket)

    parsed = raw.flat_map(
        parse_event,
        output_type=Types.TUPLE([
            Types.INT(), Types.STRING(), Types.INT(),
            Types.FLOAT(), Types.LONG(),
        ]),
    )

    wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(args.watermark_lateness_sec))
        .with_timestamp_assigner(EventTimeAssigner())
    )
    events = parsed.assign_timestamps_and_watermarks(wm)

    plans = [
        ("q2_1h",     Time.hours(1)),
        ("q2_6h",     Time.hours(6)),
        ("q2_global", Time.days(120)),
    ]

    keyed = events.key_by(lambda x: x[0], key_type=Types.INT())

    for subdir, window_size in plans:
        out_dir = os.path.join(args.output_root, subdir)
        agg = (
            keyed
            .window(TumblingEventTimeWindows.of(window_size))
            .aggregate(
                AirportAggregator(),
                FormatAirportFn(),
                accumulator_type=Types.PICKLED_BYTE_ARRAY(),
                output_type=Types.STRING(),
            )
        )
        agg.sink_to(make_sink(out_dir, subdir))

    env.execute("SABD-P2-Q2-preagg")


if __name__ == "__main__":
    main()
