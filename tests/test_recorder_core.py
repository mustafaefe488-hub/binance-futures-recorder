"""Recorder dispatch and connection management with injected fake sockets (no network)."""

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from websockets.exceptions import ConnectionClosedError

from binance_futures_recorder import core
from binance_futures_recorder.book import BookState
from binance_futures_recorder.core import ConnectionManager, Received, Recorder
from binance_futures_recorder.streams import Route, StreamKind
from binance_futures_recorder.writer import raw_path, read_lines

TS = 1_793_427_896_000  # 2026-10-31 UTC
DAY = "2026-10-31"


def wrap(stream: str, data: dict[str, Any]) -> str:
    return json.dumps({"stream": stream, "data": data})


def depth(first: int, last: int, prev: int, e: int = TS) -> str:
    data = {"e": "depthUpdate", "E": e, "T": e, "s": "BTCUSDT", "U": first, "u": last, "pu": prev}
    return wrap("btcusdt@depth@100ms", {**data, "b": [["99.0", "1"]], "a": [["101.0", "1"]]})


def agg(a: int, e: int = TS) -> str:
    data = {"e": "aggTrade", "E": e, "s": "BTCUSDT", "a": a, "p": "100.0", "q": "0.001"}
    return wrap("btcusdt@aggTrade", {**data, "nq": "0.001", "f": a, "l": a, "T": e, "m": True})


def kline(t: int, e: int) -> str:
    k = {"t": t, "T": t + 59_999, "s": "BTCUSDT", "i": "1m", "f": 1, "L": 9, "o": "1.0"}
    k |= {"c": "1.1", "h": "1.2", "l": "0.9", "v": "5.000", "n": 9, "x": False}
    k |= {"q": "5.50000", "V": "2.000", "Q": "2.20000", "B": "0"}
    return wrap("btcusdt@kline_1m", {"e": "kline", "E": e, "s": "BTCUSDT", "k": k})


def rec(raw: str, ts: int = TS, conn: int = 1) -> Received:
    return Received(raw, ts, 123, conn, Route.PUBLIC)


SNAP = {"lastUpdateId": 100, "E": TS, "T": TS, "bids": [["99.0", "2"]], "asks": [["101.0", "2"]]}


@pytest.fixture(autouse=True)
def fixed_wall_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the recorder's wall clock to TS so records stamped with "now" (snapshots, events,
    book checks, receive times) land in DAY regardless of the date the tests run on."""
    monkeypatch.setattr(core, "wall_ms", lambda: TS + 500)


@pytest.fixture
def recorder(tmp_path: Path) -> Recorder:
    return Recorder(["BTCUSDT"], tmp_path, log=lambda *a, **k: None, fetch_json=lambda url: SNAP)


def lines(root: Path, symbol: str, stream: str, day: str = DAY) -> list[dict[str, Any]]:
    path = raw_path(root, day, symbol, stream)
    return [json.loads(x) for x in read_lines(path)] if path.exists() else []


def test_raw_lines_carry_local_timestamps(recorder: Recorder, tmp_path: Path) -> None:
    recorder.handle(Received(agg(1), TS + 80, 987654321, 3, Route.MARKET))
    recorder.writer.flush()
    [line] = lines(tmp_path, "BTCUSDT", "aggTrade")
    assert line["local_ts_ms"] == TS + 80
    assert line["local_mono_ns"] == 987654321
    assert line["conn"] == 3
    assert line["msg"]["data"]["a"] == 1
    assert recorder.latency.summary_and_reset()["aggTrade"]["p50"] == 80


async def test_snapshot_bridge_and_records(recorder: Recorder, tmp_path: Path) -> None:
    recorder.handle(rec(depth(95, 103, 94)))
    await recorder.fetch_and_apply_snapshot("BTCUSDT", "initial")
    assert recorder.books["BTCUSDT"].state is BookState.SYNCED
    recorder.handle(rec(depth(104, 108, 103)))
    recorder.handle(rec(depth(120, 125, 119)))  # pu break
    assert recorder.books["BTCUSDT"].state is BookState.NEEDS_SNAPSHOT
    recorder.book_check()  # not synced -> nothing written
    recorder.writer.flush()
    assert len(lines(tmp_path, "BTCUSDT", "depth")) == 3  # raw diffs are always kept
    [snap] = lines(tmp_path, "BTCUSDT", "depthSnapshot")
    assert snap["reason"] == "initial"
    assert snap["snapshot"]["lastUpdateId"] == 100
    events = [e["event"] for e in lines(tmp_path, "BTCUSDT", "events")]
    assert events == ["synced", "resync"]
    assert lines(tmp_path, "BTCUSDT", "events")[1]["detail"]["reason"] == "pu_mismatch"


async def test_book_check_writes_top_levels(recorder: Recorder, tmp_path: Path) -> None:
    recorder.handle(rec(depth(95, 103, 94)))
    await recorder.fetch_and_apply_snapshot("BTCUSDT", "initial")
    recorder.book_check()
    recorder.writer.flush()
    [check] = lines(tmp_path, "BTCUSDT", "bookCheck")
    assert check["lastUpdateId"] == 103
    assert check["bids"] == [["99.0", "1"]]


def test_duplicates_from_overlapping_connections_dropped(
    recorder: Recorder, tmp_path: Path
) -> None:
    for raw in (agg(1), agg(2), agg(3)):
        recorder.handle(rec(raw, conn=1))
    for raw in (agg(2), agg(3), agg(4)):  # new connection replays the overlap
        recorder.handle(rec(raw, conn=2))
    force = wrap("btcusdt@forceOrder", {"e": "forceOrder", "E": TS, "o": {"s": "BTCUSDT"}})
    recorder.handle(rec(force, conn=1))
    recorder.handle(rec(force, conn=2))
    recorder.writer.flush()
    assert [x["msg"]["data"]["a"] for x in lines(tmp_path, "BTCUSDT", "aggTrade")] == [1, 2, 3, 4]
    assert len(lines(tmp_path, "BTCUSDT", "forceOrder")) == 1
    assert recorder.agg_seq["BTCUSDT"].gaps == 0


def test_agg_trade_and_kline_gaps_become_events(recorder: Recorder, tmp_path: Path) -> None:
    recorder.handle(rec(agg(10)))
    recorder.handle(rec(agg(13)))
    t = 1_793_427_840_000
    recorder.handle(rec(kline(t, TS)))
    recorder.handle(rec(kline(t, TS + 250)))
    recorder.handle(rec(kline(t + 180_000, TS + 500)))
    recorder.writer.flush()
    events = lines(tmp_path, "BTCUSDT", "events")
    assert [(e["event"], e["detail"]) for e in events] == [
        ("agg_trade_gap", {"expected": 11, "got": 13}),
        ("kline_gap", {"expected": t + 60_000, "got": t + 180_000}),
    ]


def test_midnight_rotation_by_receive_time(recorder: Recorder, tmp_path: Path) -> None:
    midnight = 1_793_491_200_000  # 2026-11-01T00:00:00Z
    recorder.handle(rec(agg(1), ts=midnight - 1))
    recorder.handle(rec(agg(2), ts=midnight))
    recorder.writer.flush()
    assert len(lines(tmp_path, "BTCUSDT", "aggTrade", "2026-10-31")) == 1
    assert len(lines(tmp_path, "BTCUSDT", "aggTrade", "2026-11-01")) == 1


def test_clock_jump_forces_resync(recorder: Recorder) -> None:
    book = recorder.books["BTCUSDT"]
    book.on_event(json.loads(depth(95, 103, 94))["data"])
    book.apply_snapshot(SNAP)
    recorder.force_resync("wall_clock_jump", d_wall_ms=5000.0)
    assert book.state is BookState.NEEDS_SNAPSHOT
    assert recorder.totals["event:resync"] == 1


# ------------------------------------------------------------- connections


class FakeWs:
    def __init__(
        self,
        messages: list[str],
        delay: float = 0.01,
        hold: bool = True,
        fail: Exception | None = None,
    ) -> None:
        self.messages = messages
        self.delay = delay
        self.hold = hold
        self.fail = fail  # raised after the messages, like an abnormal close
        self.closed = False

    def __aiter__(self) -> AsyncIterator[str]:
        return self._gen()

    async def _gen(self) -> AsyncIterator[str]:
        for m in self.messages:
            await asyncio.sleep(self.delay)
            if self.closed:
                return
            yield m
        if self.fail is not None:
            raise self.fail
        if self.hold:
            await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True


def fake_connect(sockets: list[FakeWs]) -> Any:
    queue = iter(sockets)

    async def connect(url: str, logger: Any) -> FakeWs:
        return next(queue)

    return connect


def manager(recorder: Recorder, sockets: list[FakeWs], log: Any = None, **kw: Any) -> Any:
    return ConnectionManager(
        Route.MARKET,
        "wss://x",
        recorder.handle,
        log or (lambda *a, **k: None),
        connect=fake_connect(sockets),
        on_close=recorder.on_connection_closed,
        switched=recorder.switched,
        **kw,
    )


def agg_ids(root: Path) -> list[int]:
    return [x["msg"]["data"]["a"] for x in lines(root, "BTCUSDT", "aggTrade")]


async def test_rollover_successor_behind(recorder: Recorder, tmp_path: Path) -> None:
    old = FakeWs([agg(1), agg(2), agg(3)])
    new = FakeWs([agg(2), agg(3), agg(4), agg(5)])
    mgr = manager(recorder, [old, new], max_age_s=0.1, verify_timeout_s=1.0)
    task = asyncio.create_task(mgr.run())
    await asyncio.sleep(0.35)
    assert mgr.rollovers == 1
    assert old.closed
    assert not new.closed
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert new.closed  # closed on shutdown
    recorder.writer.flush()
    assert agg_ids(tmp_path) == [1, 2, 3, 4, 5]
    assert recorder.agg_seq["BTCUSDT"].gaps == 0


async def test_rollover_successor_ahead_loses_nothing(recorder: Recorder, tmp_path: Path) -> None:
    """The successor already delivers 4..7 while the old connection is still at 2."""
    old = FakeWs([agg(i) for i in range(1, 7)], delay=0.05)
    new = FakeWs([agg(i) for i in range(4, 8)], delay=0.005)
    mgr = manager(recorder, [old, new], max_age_s=0.12, verify_timeout_s=2.0)
    task = asyncio.create_task(mgr.run())
    await asyncio.sleep(0.6)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    recorder.writer.flush()
    assert mgr.rollovers == 1
    assert agg_ids(tmp_path) == [1, 2, 3, 4, 5, 6, 7]
    assert recorder.agg_seq["BTCUSDT"].gaps == 0
    conns = [x["conn"] for x in lines(tmp_path, "BTCUSDT", "aggTrade")]
    assert conns == [conns[0]] * 4 + [conns[0] + 1] * 3  # old up to the overlap, then successor


async def test_rollover_keeps_old_connection_if_new_is_silent(recorder: Recorder) -> None:
    old = FakeWs([agg(1)])
    silent = FakeWs([])
    mgr = manager(recorder, [old, silent], max_age_s=0.05, verify_timeout_s=0.1)
    task = asyncio.create_task(mgr.run())
    await asyncio.sleep(0.3)
    assert mgr.rollovers == 0
    assert silent.closed
    assert not old.closed
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_disconnect_reconnects(recorder: Recorder) -> None:
    first = FakeWs([agg(1), agg(2)], hold=False)  # stream ends -> disconnect
    second = FakeWs([agg(3)])
    events: list[str] = []
    mgr = manager(recorder, [first, second], log=lambda e, **k: events.append(e))
    task = asyncio.create_task(mgr.run())
    await asyncio.sleep(1.3)  # first backoff is 1 s
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert events[:3] == ["ws_connected", "ws_disconnected", "ws_connected"]
    assert mgr.disconnects == 1
    assert recorder.totals["msg:aggTrade"] == 3


async def test_abnormal_close_reconnects(recorder: Recorder) -> None:
    """Regression: the server dropped the socket without a close frame,
    the reader died with ConnectionClosedError and ``_close`` re-raised it, killing the process."""
    first = FakeWs([agg(1), agg(2)], fail=ConnectionClosedError(None, None))
    second = FakeWs([agg(3)])
    events: list[tuple[str, dict[str, Any]]] = []
    mgr = manager(recorder, [first, second], log=lambda e, **k: events.append((e, k)))
    task = asyncio.create_task(mgr.run())
    await asyncio.sleep(1.3)  # first backoff is 1 s
    assert not task.done()  # the manager survived the abnormal close
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert [e for e, _ in events][:3] == ["ws_connected", "ws_disconnected", "ws_connected"]
    assert "ConnectionClosedError" in events[1][1]["error"]
    assert mgr.disconnects == 1
    assert recorder.totals["msg:aggTrade"] == 3


async def test_bad_message_does_not_drop_connection(recorder: Recorder) -> None:
    ws = FakeWs(["not json", wrap("btcusdt@unknown", {"E": 1}), agg(1)])
    mgr = manager(recorder, [ws])
    task = asyncio.create_task(mgr.run())
    await asyncio.sleep(0.15)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert mgr.handle_errors == 2
    assert mgr.disconnects == 0
    assert recorder.totals["msg:aggTrade"] == 1


class _Reader:
    def __init__(self) -> None:
        self.cancelled = False

    def done(self) -> bool:
        return self.cancelled

    def cancel(self) -> None:
        self.cancelled = True


def test_silent_stream_forces_reconnect(recorder: Recorder) -> None:
    mgr = manager(recorder, [])
    reader = _Reader()
    mgr.current = SimpleNamespace(opened_mono_ns=0, reader=reader)
    recorder.managers = [mgr]
    now = 100 * 10**9
    recorder.last_msg_mono["btcusdt@markPrice@1s"] = now - 40 * 10**9  # silent 40 s
    recorder.last_msg_mono["btcusdt@aggTrade"] = now - 400 * 10**9  # trades may pause: ignored
    assert recorder.check_stalls(now) == ["btcusdt@markPrice@1s"]
    assert reader.cancelled
    assert mgr.forced_reconnects == 1


def test_write_error_keeps_lines_for_next_flush(recorder: Recorder, tmp_path: Path) -> None:
    blocker = tmp_path / "raw" / DAY
    blocker.parent.mkdir(parents=True)
    blocker.write_text("not a directory")  # makes the day directory impossible to create
    recorder.handle(rec(agg(1)))
    recorder.handle(rec(agg(2)))
    recorder.flush()
    assert recorder.totals["write_errors"] == 1
    blocker.unlink()
    recorder.handle(rec(agg(3)))
    recorder.flush()
    assert agg_ids(tmp_path) == [1, 2, 3]


@pytest.mark.network
async def test_live_streams_for_ten_seconds(tmp_path: Path) -> None:
    """Connects to Binance for 10 s; every stream except forceOrder (event-driven) delivers."""
    r = Recorder(["BTCUSDT", "ETHUSDT"], tmp_path, log=lambda *a, **k: None)
    await r.run(duration_s=10, flush_s=1)
    for symbol in ("BTCUSDT", "ETHUSDT"):
        for kind in StreamKind:
            if kind is StreamKind.FORCE_ORDER:
                continue
            day_dirs = list((tmp_path / "raw").iterdir())
            n = sum(len(lines(tmp_path, symbol, kind.value, d.name)) for d in day_dirs)
            assert n >= 1, f"{symbol} {kind.value}: no messages"
        assert r.books[symbol].state is BookState.SYNCED


async def test_ping_watch_counts_server_pings_through_websockets() -> None:
    """Local websockets server pings 3 times; the PingWatch hook must see all of them."""
    from websockets.asyncio.server import serve  # noqa: PLC0415

    from binance_futures_recorder.core import PingWatch, websockets_connect  # noqa: PLC0415

    async def handler(ws: Any) -> None:
        for _ in range(3):
            await ws.ping()
            await asyncio.sleep(0.05)
        await ws.send("done")
        await asyncio.sleep(0.5)

    async with serve(handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        watch = PingWatch()
        ws = await websockets_connect(f"ws://127.0.0.1:{port}", watch)
        async for _ in ws:
            break
        await ws.close()
    assert watch.pings == 3
    assert 0 < watch.max_interval_s < 1


def test_heartbeat_carries_disk_threshold(recorder: Recorder, tmp_path: Path) -> None:
    recorder.min_free_gb = 8.0
    recorder.write_heartbeat({}, 40.0)
    hb = json.loads((tmp_path / "heartbeat.json").read_text(encoding="utf-8"))
    assert hb["min_free_gb"] == 8.0
    assert hb["disk_free_gb"] == 40.0
