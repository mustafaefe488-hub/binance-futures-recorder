"""Shared definitions: Arrow column types, epoch-unit normalisation, symbols, data root."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

# Data root: the current working directory unless RECORDER_ROOT is set.
DATA_ROOT = Path(os.environ.get("RECORDER_ROOT") or Path.cwd())

SYMBOLS = ("BTCUSDT", "ETHUSDT")

TS = pa.timestamp("ms", tz="UTC")
PX = pa.decimal128(18, 8)
QV = pa.decimal128(24, 8)
INT = pa.int64()


class DataValidationError(Exception):
    """Archive content violates an expected property (schema, order, units)."""


def epoch_unit(lo: int, hi: int) -> str:
    """Infer the epoch unit of a timestamp column from its magnitude (s, ms or us)."""
    bands = {"s": (10**8, 10**11), "ms": (10**11, 10**14), "us": (10**14, 10**17)}
    for unit, (a, b) in bands.items():
        if a <= lo and hi < b:
            return unit
    raise DataValidationError(f"cannot infer epoch unit for range [{lo}, {hi}]")


def to_epoch_ms(values: pa.Array) -> pa.Array:
    """int64 epoch values (s/ms/us, detected) -> int64 milliseconds."""
    if len(values) == 0 or values.null_count == len(values):
        return values
    mm = pc.min_max(values)
    unit = epoch_unit(int(mm["min"].as_py()), int(mm["max"].as_py()))
    if unit == "s":
        return pc.multiply(values, 1000)
    if unit == "us":
        return pc.divide(values, 1000)
    return values
