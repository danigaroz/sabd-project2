"""Small helpers for PyFlink jobs."""
from datetime import datetime
from pyflink.common import Types


# Flink Row schema for our flight events (matches preprocess.py output order)
EVENT_FIELD_NAMES = [
    "YEAR", "MONTH", "DAY_OF_MONTH",
    "OP_UNIQUE_CARRIER",
    "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
    "DEP_DELAY", "ARR_DELAY",
    "CANCELLED", "DIVERTED",
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
    "event_time",
]

EVENT_FIELD_TYPES = [
    Types.INT(), Types.INT(), Types.INT(),
    Types.STRING(),
    Types.INT(), Types.INT(),
    Types.INT(),
    Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
    Types.DOUBLE(), Types.DOUBLE(),
    Types.LONG(),  # event_time as ms epoch
]


def parse_csv_line(line: str) -> dict:
    """Parse a single events.csv line into a typed dict. Header skipped externally."""
    parts = line.split(",")
    def _f(s):
        return float(s) if s.strip() != "" else 0.0
    def _i(s):
        return int(float(s)) if s.strip() != "" else 0
    return {
        "YEAR": _i(parts[0]),
        "MONTH": _i(parts[1]),
        "DAY_OF_MONTH": _i(parts[2]),
        "OP_UNIQUE_CARRIER": parts[3],
        "ORIGIN_AIRPORT_ID": _i(parts[4]),
        "DEST_AIRPORT_ID": _i(parts[5]),
        "CRS_DEP_TIME": _i(parts[6]),
        "DEP_DELAY": _f(parts[7]),
        "ARR_DELAY": _f(parts[8]),
        "CANCELLED": _f(parts[9]),
        "DIVERTED": _f(parts[10]),
        "CARRIER_DELAY": _f(parts[11]),
        "WEATHER_DELAY": _f(parts[12]),
        "NAS_DELAY": _f(parts[13]),
        "SECURITY_DELAY": _f(parts[14]),
        "LATE_AIRCRAFT_DELAY": _f(parts[15]),
        "event_time": _i(parts[16]),
    }


def fmt_ts(ms: int) -> str:
    """Format a ms-epoch as YYYY-MM-DD HH:MM:SS (UTC)."""
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
