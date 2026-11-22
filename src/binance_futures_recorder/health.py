"""One-shot health check of a running recorder, from data/live/heartbeat.json.

  uv run python -m binance_futures_recorder.health

Exit code 0 = healthy, 1 = problem (stale heartbeat, unsynced book, late server ping, low disk).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from binance_futures_recorder.writer import LIVE_ROOT

STALE_S = 180.0  # heartbeat is written every 60 s
PING_LATE_S = 240.0  # server pings every 180 s


def problems(hb: dict[str, Any], now_s: float, min_free_gb: float) -> list[str]:
    out = []
    age = now_s - hb["ts_ms"] / 1000
    if age > STALE_S:
        out.append(f"heartbeat is {age:.0f} s old (recorder stopped or hung)")
    for symbol, state in hb["books"].items():
        if state != "synced":
            out.append(f"{symbol} book is {state}")
    for route, c in hb["connections"].items():
        since = c.get("since_last_ping_s")
        if since is not None and since > PING_LATE_S:
            out.append(f"{route}: last server ping {since:.0f} s ago")
    if hb.get("disk_free_gb", min_free_gb) < min_free_gb:
        out.append(f"disk free {hb['disk_free_gb']} GB < {min_free_gb} GB")
    return out


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m binance_futures_recorder.health")
    p.add_argument("--root", type=Path, default=LIVE_ROOT)
    p.add_argument(
        "--min-free-gb", type=float, help="default: the recorder's own threshold from heartbeat"
    )
    args = p.parse_args(argv)
    path = args.root / "heartbeat.json"
    if not path.is_file():
        print(f"UNHEALTHY: no heartbeat at {path}")
        return 1
    hb = json.loads(path.read_text(encoding="utf-8"))
    threshold = args.min_free_gb if args.min_free_gb is not None else hb.get("min_free_gb", 20.0)
    found = problems(hb, time.time(), float(threshold))
    print(f"uptime {hb['uptime_s'] / 3600:.1f} h, books {hb['books']}, resyncs {hb['resyncs']}")
    print(f"aggTrade gaps {hb['agg_trade_gaps']}, kline gaps {hb['kline_gaps']}")
    for route, c in hb["connections"].items():
        print(
            f"{route}: conn {c['conn']}, disconnects {c['disconnects']}, "
            f"rollovers {c['rollovers']}, server pings {c['server_pings']} "
            f"(max gap {c['max_ping_interval_s']} s)"
        )
    print(f"last minute: {hb['last_minute']['messages']}, disk free {hb['disk_free_gb']} GB")
    if found:
        print("UNHEALTHY: " + "; ".join(found))
        return 1
    print("HEALTHY")
    return 0


if __name__ == "__main__":
    sys.exit(main())
