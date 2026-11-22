"""Run the live recorder.

  uv run python -m binance_futures_recorder --symbols BTCUSDT ETHUSDT
  uv run python -m binance_futures_recorder --duration 1800        # 30-minute test run

Optional: RECORDER_HEALTHCHECKS_URL=<ping url> pings Healthchecks.io once a minute.
Ctrl+C stops cleanly (buffers flushed, files closed). Logs go to data/live/logs/ (add --echo
to also print them; off by default because a blocked Windows console would stall recording).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from binance_futures_recorder._shared import SYMBOLS
from binance_futures_recorder.core import DEFAULT_MAX_AGE_S, JsonLog, Recorder
from binance_futures_recorder.writer import LIVE_ROOT


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m binance_futures_recorder", description=__doc__)
    p.add_argument("--symbols", nargs="+", default=list(SYMBOLS), choices=SYMBOLS)
    p.add_argument("--root", type=Path, default=LIVE_ROOT)
    p.add_argument("--flush-seconds", type=float, default=5.0)
    p.add_argument("--rollover-hours", type=float, default=DEFAULT_MAX_AGE_S / 3600)
    p.add_argument("--min-free-gb", type=float, default=20.0)
    p.add_argument("--duration", type=float, help="stop after N seconds (test runs)")
    p.add_argument(
        "--echo", action="store_true", help="also print log lines (interactive use only)"
    )
    args = p.parse_args(argv)
    if not 0 < args.rollover_hours < 24:
        p.error("--rollover-hours must be below the 24 h server limit")
    recorder = Recorder(args.symbols, args.root, log=JsonLog(args.root, echo=args.echo))
    # asyncio.run cancels the tasks on Ctrl+C; Recorder.run flushes in its finally
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(
            recorder.run(
                flush_s=args.flush_seconds,
                max_age_s=args.rollover_hours * 3600,
                min_free_gb=args.min_free_gb,
                healthcheck_url=os.environ.get("RECORDER_HEALTHCHECKS_URL") or None,
                duration_s=args.duration,
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
