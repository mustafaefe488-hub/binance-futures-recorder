import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from binance_futures_recorder.monitor import (
    JumpDetector,
    LatencyStats,
    SequenceGap,
    measure_offset,
    percentiles,
)
from binance_futures_recorder.streams import (
    Route,
    StreamKind,
    dedupe_key,
    parse_stream,
    route_url,
    stream_name,
)
from binance_futures_recorder.writer import DailyZstdWriter, raw_path, read_lines


def test_stream_names_and_routes() -> None:
    assert stream_name("BTCUSDT", StreamKind.DEPTH) == "btcusdt@depth@100ms"
    assert stream_name("ETHUSDT", StreamKind.MARK_PRICE) == "ethusdt@markPrice@1s"
    assert parse_stream("ethusdt@kline_1m") == ("ETHUSDT", StreamKind.KLINE_1M)
    pub = route_url(Route.PUBLIC, ["BTCUSDT"])
    mkt = route_url(Route.MARKET, ["BTCUSDT"])
    assert (
        pub
        == "wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker"
    )
    assert mkt.startswith("wss://fstream.binance.com/market/stream?streams=")
    assert {"aggTrade", "markPrice@1s", "forceOrder", "kline_1m"} == {
        s.split("@", 1)[1] for s in mkt.split("=", 1)[1].split("/")
    }
    with pytest.raises(ValueError):
        parse_stream("btcusdt@trade")


def test_dedupe_keys() -> None:
    assert dedupe_key(StreamKind.DEPTH, {"u": 7}) == 7
    assert dedupe_key(StreamKind.AGG_TRADE, {"a": 9}) == 9
    assert dedupe_key(StreamKind.KLINE_1M, {"E": 11}) == 11
    assert dedupe_key(StreamKind.FORCE_ORDER, {"E": 1}) is None


def test_percentiles_nearest_rank() -> None:
    values = [float(v) for v in range(1, 101)]
    assert percentiles(values) == {"p50": 50.0, "p95": 95.0, "p99": 99.0}
    assert percentiles([5.0]) == {"p50": 5.0, "p95": 5.0, "p99": 5.0}
    assert percentiles([]) == {}


def test_latency_stats_reset() -> None:
    lat = LatencyStats()
    for d in (10, 20, 30, 40):
        lat.add("depth", 1000 + d, 1000)
    summary = lat.summary_and_reset()
    assert summary["depth"]["n"] == 4
    assert summary["depth"]["p50"] == 20
    assert lat.summary_and_reset() == {}


def test_sequence_gap_agg_trade_ids() -> None:
    seq = SequenceGap(step=1)
    assert [seq.observe(v) for v in (10, 11, 12)] == [None, None, None]
    assert seq.observe(15) == (13, 15)
    assert (seq.gaps, seq.missing) == (1, 2)
    assert seq.observe(14) == (16, 14)  # out of order: reported, high-water mark kept
    assert seq.observe(16) is None


def test_sequence_gap_kline_open_times() -> None:
    seq = SequenceGap(step=60_000, same_ok=True)
    t = 1_793_427_840_000
    assert [seq.observe(x) for x in (t, t, t + 60_000, t + 60_000)] == [None] * 4
    assert seq.observe(t + 240_000) == (t + 120_000, t + 240_000)
    assert seq.missing == 2


def test_jump_detector() -> None:
    det = JumpDetector(jump_ms=1000, stall_ms=10_000)
    assert det.check(1_000_000.0, 0) is None
    assert det.check(1_001_000.0, 1_000_000_000) is None  # 1 s, both clocks agree
    hit = det.check(1_007_000.0, 2_000_000_000)  # wall +6 s, mono +1 s
    assert hit is not None
    assert hit[0] == "wall_clock_jump"
    hit = det.check(1_037_000.0, 32_000_000_000)  # both +30 s: suspended/stalled
    assert hit is not None
    assert hit[0] == "process_stall"


def test_measure_offset_uses_request_midpoint() -> None:
    clock = iter([1000.0, 1100.0])
    sample = measure_offset(
        fetch=lambda url: json.dumps({"serverTime": 1300}).encode(), wall_ms=lambda: next(clock)
    )
    assert sample.offset_ms == 250.0
    assert sample.rtt_ms == 100.0


def test_writer_frames_and_midnight_rotation(tmp_path: Path) -> None:
    w = DailyZstdWriter(tmp_path)
    before = int(datetime(2026, 10, 31, 23, 59, 59, 900_000, tzinfo=UTC).timestamp() * 1000)
    after = before + 200
    w.write(before, "BTCUSDT", "aggTrade", '{"n":1}')
    w.flush()
    w.write(before + 50, "BTCUSDT", "aggTrade", '{"n":2}')
    w.write(after, "BTCUSDT", "aggTrade", '{"n":3}')
    w.flush()
    oct31 = raw_path(tmp_path, "2026-10-31", "BTCUSDT", "aggTrade")
    nov1 = raw_path(tmp_path, "2026-11-01", "BTCUSDT", "aggTrade")
    assert list(read_lines(oct31)) == ['{"n":1}', '{"n":2}']  # two frames, one file
    assert list(read_lines(nov1)) == ['{"n":3}']
    assert w.lines_written["aggTrade"] == 3
    assert w.pending() == 0


def test_health_problems() -> None:
    from binance_futures_recorder.health import problems  # noqa: PLC0415

    hb = {
        "ts_ms": 1_000_000,
        "books": {"BTCUSDT": "synced", "ETHUSDT": "needs_snapshot"},
        "connections": {"public": {"since_last_ping_s": 300.0}, "market": {}},
        "disk_free_gb": 5.0,
    }
    found = problems(hb, now_s=1_000 + 400, min_free_gb=20)
    assert len(found) == 4
    assert problems({**hb, "books": {}, "connections": {}, "disk_free_gb": 50}, 1_010, 20) == []
