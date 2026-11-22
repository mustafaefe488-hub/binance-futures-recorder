"""Append-only daily JSONL files compressed with zstd.

Layout: {root}/raw/{YYYY-MM-DD}/{SYMBOL}_{stream}.jsonl.zst  (UTC date of local receive time)

Why zstd rather than gzip: roughly 3-5x faster compression at a better ratio on JSON (the
recorder writes ~300 msg/s continuously on a home PC), and the same codec as our Parquet.
Every flush appends one complete zstd frame, so a crash loses at most the unflushed buffer
and the file stays readable (readers decode across concatenated frames). Rotation at 00:00
UTC is implicit: the date in the path comes from each record's own timestamp.
"""

from __future__ import annotations

import io
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import zstandard as zstd

from binance_futures_recorder._shared import DATA_ROOT

LIVE_ROOT = DATA_ROOT / "data" / "live"


def day_of(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%d")


def raw_path(root: Path, day: str, symbol: str, stream: str) -> Path:
    return root / "raw" / day / f"{symbol}_{stream}.jsonl.zst"


class DailyZstdWriter:
    def __init__(self, root: Path, level: int = 3) -> None:
        self.root = root
        self._compressor = zstd.ZstdCompressor(level=level)
        self._buffers: dict[tuple[str, str, str], list[str]] = defaultdict(list)
        self.bytes_written: dict[str, int] = defaultdict(int)  # compressed, per stream
        self.lines_written: dict[str, int] = defaultdict(int)

    def write(self, ts_ms: int, symbol: str, stream: str, line: str) -> None:
        self._buffers[(day_of(ts_ms), symbol, stream)].append(line)

    def pending(self) -> int:
        return sum(len(v) for v in self._buffers.values())

    def flush(self) -> list[str]:
        """Append one frame per file; on an I/O error the lines stay buffered for the next
        flush. Returns the error messages (empty on success)."""
        buffers, self._buffers = self._buffers, defaultdict(list)
        errors: list[str] = []
        for (day, symbol, stream), lines in buffers.items():
            if not lines:
                continue
            path = raw_path(self.root, day, symbol, stream)
            frame = self._compressor.compress(("\n".join(lines) + "\n").encode("utf-8"))
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("ab") as fh:
                    fh.write(frame)
            except OSError as err:
                self._buffers[(day, symbol, stream)][:0] = lines  # keep order, retry later
                errors.append(f"{path}: {err!r}")
                continue
            self.bytes_written[stream] += len(frame)
            self.lines_written[stream] += len(lines)
        return errors


def read_lines(path: Path) -> Iterator[str]:
    """Iterate the JSON lines of a (possibly multi-frame) .jsonl.zst file."""
    with path.open("rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
        for line in io.TextIOWrapper(reader, encoding="utf-8"):
            if line.strip():
                yield line.rstrip("\n")
