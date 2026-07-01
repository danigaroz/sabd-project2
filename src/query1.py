"""SABD Project 2 — Query 1 (PyFlink).

Consumes the flight event stream from the feeder via TCP socket,
applies a 1-hour tumbling event-time window per airline (AA, DL, UA, WN)
and computes:

    num_flights, completed, cancelled, diverted,
    dep_delay_mean (non-cancelled only),
    cancellation_rate (cancelled / total),
    late_departure_rate (non-cancelled w/ DEP_DELAY > 15 / non-cancelled)

Output: CSV file in /opt/flink/app/Results/q1.csv

Pipeline (DataStream API):
    socket_text_stream
      -> flat_map (parse + filter to 4 carriers)
      -> assign_timestamps_and_watermarks (bounded out-of-orderness 60s)
      -> key_by (airline)
      -> window (TumblingEventTimeWindow 1h)
      -> aggregate (incremental + window function)
      -> map to CSV line
      -> sink file
"""
import argparse
import os
from datetime import datetime

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.connectors.file_system import (
    FileSink, OutputFileConfig, RollingPolicy,
)
from pyflink.datastream.functions import (
    AggregateFunction, ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time

TARGET_CARRIERS = {"AA", "DL", "UA", "WN"}


# ----- parser ---------------------------------------------------------------

def parse_event(line: str):
    """Parse one CSV line from the feeder. Yields 0 or 1 tuple.

    Using a generator (flat_map) so we can drop unwanted records without
    violating the downstream output_type.
    """
    try:
        p = line.strip().split(",")
        if len(p) < 17:
            return
        carrier = p[3]
        if carrier not in TARGET_CARRIERS:
            return
        def _f(s):
            return float(s) if s.strip() != "" else 0.0
        yield (
            carrier,                # airline
            _f(p[7]),               # DEP_DELAY
            _f(p[9]),               # CANCELLED
            _f(p[10]),              # DIVERTED
            int(p[16]),             # event_time (ms)
        )
    except Exception:
        return


# ----- timestamp assigner ---------------------------------------------------

class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # value[4] is event_time in ms
        return value[4]


# ----- aggregator -----------------------------------------------------------

class Q1Aggregator(AggregateFunction):
    """Incremental aggregator.

    Accumulator: (num_flights, completed, cancelled, diverted,
                  sum_dep_delay_non_cancelled, count_non_cancelled,
                  late_non_cancelled)
    """

    def create_accumulator(self):
        return (0, 0, 0, 0, 0.0, 0, 0)

    def add(self, value, acc):
        _, dep_delay, cancelled, diverted, _ = value
        is_cancelled = cancelled >= 1.0
        is_diverted = diverted >= 1.0
        is_completed = not is_cancelled and not is_diverted

        sum_dd = acc[4]
        cnt_nc = acc[5]
        late_nc = acc[6]
        if not is_cancelled:
            sum_dd += dep_delay
            cnt_nc += 1
            if dep_delay > 15.0:
                late_nc += 1

        return (
            acc[0] + 1,
            acc[1] + (1 if is_completed else 0),
            acc[2] + (1 if is_cancelled else 0),
            acc[3] + (1 if is_diverted else 0),
            sum_dd,
            cnt_nc,
            late_nc,
        )

    def get_result(self, acc):
        return acc

    def merge(self, a, b):
        return tuple(x + y for x, y in zip(a, b))


# ----- window function (emits one CSV row per window+airline) ---------------

class Q1WindowFn(ProcessWindowFunction):
    def process(self, key, ctx, elements):
        acc = next(iter(elements))
        num, completed, cancelled, diverted, sum_dd, cnt_nc, late_nc = acc
        if num == 0:
            return
        dep_delay_mean = (sum_dd / cnt_nc) if cnt_nc > 0 else 0.0
        cancellation_rate = (cancelled / num) * 100.0
        late_departure_rate = (late_nc / cnt_nc) * 100.0 if cnt_nc > 0 else 0.0

        ws = ctx.window().start
        we = ctx.window().end

        def fmt(ms):
            return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

        line = (f"{fmt(ws)},{fmt(we)},{key},"
                f"{num},{completed},{cancelled},{diverted},"
                f"{dep_delay_mean:.2f},{cancellation_rate:.2f},"
                f"{late_departure_rate:.2f}")
        yield line


# ----- main -----------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="feeder")
    ap.add_argument("--port", type=int, default=9999)
    ap.add_argument("--output", default="/opt/flink/app/Results/q1")
    ap.add_argument("--parallelism", type=int, default=1)
    ap.add_argument("--watermark-lateness-sec", type=int, default=60,
                    help="max bounded out-of-orderness in event-time seconds")
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    # Source: socket text stream.
    # NOTE: PyFlink 1.20 doesn't expose env.socket_text_stream() in the
    # Python API; we reach the underlying Java StreamExecutionEnvironment
    # via the j-handle. The semantics are identical to Java's
    # SocketTextStreamFunction (one line per event, no replay).
    j_socket = env._j_stream_execution_environment.socketTextStream(
        args.host, args.port
    )
    raw = DataStream(j_socket)

    # Parse + filter to 4 target carriers (flat_map drops unwanted lines).
    parsed = raw.flat_map(
        parse_event,
        output_type=Types.TUPLE([
            Types.STRING(), Types.FLOAT(), Types.FLOAT(),
            Types.FLOAT(), Types.LONG(),
        ]),
    )

    # Event-time + watermark strategy: events arrive sorted by construction,
    # we add a small bounded out-of-orderness as defensive margin.
    wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(args.watermark_lateness_sec))
        .with_timestamp_assigner(EventTimeAssigner())
    )
    events = parsed.assign_timestamps_and_watermarks(wm)

    # Key by airline, 1-hour tumbling event-time window, aggregate.
    csv_lines = (
        events
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .aggregate(Q1Aggregator(), Q1WindowFn(),
                   accumulator_type=Types.TUPLE([
                       Types.INT(), Types.INT(), Types.INT(), Types.INT(),
                       Types.DOUBLE(), Types.INT(), Types.INT(),
                   ]),
                   output_type=Types.STRING())
    )

    # Sink: rolled CSV files. Encoder.simple_string_encoder writes
    # one record per call followed by a line separator.
    sink = (
        FileSink
        .for_row_format(args.output, Encoder.simple_string_encoder())
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("q1")
            .with_part_suffix(".csv")
            .build()
        )
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )
    csv_lines.sink_to(sink)

    env.execute("SABD-P2-Q1")


if __name__ == "__main__":
    main()
