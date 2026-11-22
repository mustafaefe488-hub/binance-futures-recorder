"""Recorder health instruments: clock offset, clock-jump detection, latency percentiles,
sequence-gap detectors and per-minute counters. Pure logic; no I/O except ``measure_offset``.
"""

from __future__ import annotations

import json
import time
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

from binance_futures_recorder.streams import REST_BASE


@dataclass(frozen=True, slots=True)
class ClockSample:
    offset_ms: float  # server - local (midpoint of the request)
    rtt_ms: float


def measure_offset(
    fetch: Callable[[str], bytes] | None = None,
    wall_ms: Callable[[], float] = lambda: time.time() * 1000,
) -> ClockSample:
    """Offset of the local wall clock against GET /fapi/v1/time."""
    get = fetch or (lambda url: urllib.request.urlopen(url, timeout=10).read())
    t0 = wall_ms()
    server = json.loads(get(f"{REST_BASE}/fapi/v1/time"))["serverTime"]
    t1 = wall_ms()
    return ClockSample(offset_ms=server - (t0 + t1) / 2, rtt_ms=t1 - t0)


class JumpDetector:
    """Detects wall-clock steps (NTP/manual) and process stalls (sleep/hibernate, freezes).

    Called about once a second with (wall ms, monotonic ns). Windows' monotonic clock keeps
    counting through sleep, so a suspend shows up as a large monotonic step (stall) rather
    than a wall/monotonic divergence; both force a book resync.
    """

    def __init__(self, jump_ms: float = 1000.0, stall_ms: float = 10_000.0) -> None:
        self.jump_ms = jump_ms
        self.stall_ms = stall_ms
        self._last: tuple[float, int] | None = None

    def check(self, wall_ms: float, mono_ns: int) -> tuple[str, dict[str, float]] | None:
        last, self._last = self._last, (wall_ms, mono_ns)
        if last is None:
            return None
        d_wall = wall_ms - last[0]
        d_mono = (mono_ns - last[1]) / 1e6
        if abs(d_wall - d_mono) > self.jump_ms:
            return "wall_clock_jump", {"d_wall_ms": d_wall, "d_mono_ms": d_mono}
        if d_mono > self.stall_ms:
            return "process_stall", {"d_wall_ms": d_wall, "d_mono_ms": d_mono}
        return None


def percentiles(values: list[float], qs: tuple[float, ...] = (50, 95, 99)) -> dict[str, float]:
    """Nearest-rank percentiles; empty input gives an empty dict."""
    if not values:
        return {}
    s = sorted(values)
    out = {}
    for q in qs:
        rank = max(1, -(-len(s) * q // 100))  # ceil(n*q/100), at least 1
        out[f"p{q:g}"] = s[int(rank) - 1]
    return out


@dataclass
class LatencyStats:
    """Local receive time minus exchange event time ``E``, per stream kind, per window."""

    samples: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))

    def add(self, kind: str, local_ms: float, event_ms: float) -> None:
        self.samples[kind].append(local_ms - event_ms)

    def summary_and_reset(self) -> dict[str, dict[str, float]]:
        out = {k: {"n": float(len(v)), **percentiles(v)} for k, v in sorted(self.samples.items())}
        self.samples = defaultdict(list)
        return out


@dataclass
class SequenceGap:
    """Detects breaks in a sequence expected to advance by ``step`` (``same_ok``: repeats are
    fine, e.g. kline updates of the current bar)."""

    step: int
    same_ok: bool = False
    last: int | None = None
    gaps: int = 0
    missing: int = 0

    def observe(self, value: int) -> tuple[int, int] | None:
        """Return (expected, got) on a gap."""
        last, self.last = self.last, value
        if last is None or (self.same_ok and value == last) or value == last + self.step:
            return None
        if value < last:
            self.last = last  # out-of-order/duplicate: keep the high-water mark
            return (last + self.step, value)
        self.gaps += 1
        self.missing += (value - last) // self.step - 1
        return (last + self.step, value)


@dataclass
class MinuteCounters:
    messages: Counter[str] = field(default_factory=Counter)
    duplicates: Counter[str] = field(default_factory=Counter)
    bytes_raw: int = 0

    def reset(self) -> dict[str, object]:
        snap = {
            "messages": dict(self.messages),
            "duplicates": dict(self.duplicates),
            "bytes_raw": self.bytes_raw,
        }
        self.messages, self.duplicates, self.bytes_raw = Counter(), Counter(), 0
        return snap
