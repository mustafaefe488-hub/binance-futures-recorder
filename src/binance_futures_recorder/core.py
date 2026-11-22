"""Recorder core: connections, dispatch, local books, periodic health tasks.

Data flow: one combined WebSocket per route (/public, /market) -> ``Recorder.handle`` (sync,
single event loop, so no locking) -> per-stream primary-connection dedupe -> raw JSONL.zst
line with ``local_ts_ms`` and ``local_mono_ns`` -> book / gap / latency bookkeeping.

Planned rollover before the 24 h server limit opens a successor connection while the old one
keeps recording. Each stream has a *primary* connection; messages from any other connection
are buffered per stream and the stream switches only when the overlap is proven (the
successor's earliest buffered key is <= the last key accepted from the old connection), so
nothing is dropped or recorded twice. Keys: depth/bookTicker ``u``, aggTrade ``a``,
markPrice/kline ``E``; forceOrder (rare, no key) is deduped by a canonical payload hash.
The old connection closes once every keyed stream has switched (or after a timeout, logged).

Review fixes (2026-10-31): per-message error isolation, guarded periodic tasks, buffered data
kept on write errors, silent-stream detection with forced reconnect, per-connection ping
stats, bounded snapshot requests honouring Retry-After, console echo off by default.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import os
import shutil
import time
import urllib.error
import urllib.request
from collections import Counter, deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from websockets.frames import Frame, Opcode

from binance_futures_recorder.book import BookNote, BookState, LocalBook
from binance_futures_recorder.monitor import (
    JumpDetector,
    LatencyStats,
    MinuteCounters,
    SequenceGap,
    measure_offset,
)
from binance_futures_recorder.streams import (
    BOOK_CHECK,
    DEPTH_SNAPSHOT,
    EVENTS,
    GLOBAL_SYMBOL,
    REST_BASE,
    SNAPSHOT_LIMIT,
    Route,
    StreamKind,
    dedupe_key,
    parse_stream,
    route_url,
    stream_name,
)
from binance_futures_recorder.writer import DailyZstdWriter, day_of

HOUR_S = 3600.0
DEFAULT_MAX_AGE_S = 23 * HOUR_S  # roll over one hour before the 24 h server limit
SERVER_PING_EXPECTED_S = 180.0  # Binance: server pings every 3 minutes, pong within 10
STABLE_CONNECTION_S = 60.0  # reconnect backoff resets only after a connection lived this long
STALL_S = 30.0  # depth (100 ms), bookTicker and markPrice (1 s) never pause this long
SNAPSHOT_TIMEOUT_S = 30.0
PENDING_MAX = 20_000  # per stream, successor messages waiting for a proven overlap
ALWAYS_TICKING = (StreamKind.DEPTH, StreamKind.BOOK_TICKER, StreamKind.MARK_PRICE)


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


def mono_ns() -> int:
    return time.monotonic_ns()


@dataclass(frozen=True, slots=True)
class Received:
    raw: str
    local_ts_ms: int
    local_mono_ns: int
    conn: int
    route: Route


class WsLike(Protocol):
    def __aiter__(self) -> AsyncIterator[str | bytes]: ...
    async def close(self) -> None: ...


type Connect = Callable[[str, logging.LoggerAdapter[logging.Logger]], Awaitable[WsLike]]


class JsonLog:
    """JSON-lines log to {root}/logs/recorder-{day}.jsonl (and stdout if ``echo``).

    Echo is off by default for unattended runs: on Windows a click in the console (QuickEdit)
    suspends console writes, and a blocked ``print`` would freeze the whole event loop.
    """

    def __init__(self, root: Path | None, echo: bool = False) -> None:
        self.root = root
        self.echo = echo
        self.write_errors = 0

    def __call__(self, event: str, **fields: Any) -> None:
        ts = wall_ms()
        line = json.dumps({"ts_ms": ts, "event": event, **fields}, default=str)
        if self.echo:
            print(line, flush=True)
        if self.root is not None:
            try:
                path = self.root / "logs" / f"recorder-{day_of(ts)}.jsonl"
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError:
                self.write_errors += 1  # logging must never stop recording


class PingWatch(logging.LoggerAdapter[logging.Logger]):
    """websockets logger that only records incoming PING frames (cheap per-frame hook).

    websockets logs ``"< %s", frame`` at DEBUG for every received frame when DEBUG is enabled.
    It wraps the given logger in its own LoggerAdapter, which forwards through ``log()``, so
    both ``log`` and ``debug`` are intercepted; every DEBUG record except server pings is
    dropped, other levels go to the real logger. One instance per connection.
    """

    def __init__(self) -> None:
        super().__init__(logging.getLogger("binance_futures_recorder.ws"), {})
        self.pings = 0
        self.last_ping_mono_ns: int | None = None
        self.max_interval_s = 0.0

    def isEnabledFor(self, level: int) -> bool:
        return level == logging.DEBUG or self.logger.isEnabledFor(level)

    def log(self, level: int, msg: object, *args: object, **kwargs: Any) -> None:
        if level == logging.DEBUG:
            self.debug(msg, *args)
        else:
            super().log(level, msg, *args, **kwargs)

    def debug(self, msg: object, *args: object, **kwargs: Any) -> None:
        if msg == "< %s" and args and isinstance(args[0], Frame) and args[0].opcode is Opcode.PING:
            now = mono_ns()
            if self.last_ping_mono_ns is not None:
                gap = (now - self.last_ping_mono_ns) / 1e9
                self.max_interval_s = max(self.max_interval_s, gap)
            self.last_ping_mono_ns = now
            self.pings += 1


async def websockets_connect(url: str, logger: logging.LoggerAdapter[logging.Logger]) -> WsLike:
    import websockets  # noqa: PLC0415 - keep import local so tests never need a network stack

    conn: WsLike = await websockets.connect(
        url, max_size=2**22, ping_interval=20, ping_timeout=20, close_timeout=2, logger=logger
    )
    return conn


@dataclass
class _Live:
    ws: WsLike
    reader: asyncio.Task[None]
    first_message: asyncio.Event
    opened_mono_ns: int
    conn_id: int
    watch: PingWatch


class ConnectionManager:
    """Keeps one combined-stream connection alive for a route, with overlapped rollover."""

    _ids = itertools.count(1)

    def __init__(  # noqa: PLR0913 - keyword-only tuning knobs
        self,
        route: Route,
        url: str,
        sink: Callable[[Received], None],
        log: Callable[..., None],
        *,
        connect: Connect = websockets_connect,
        max_age_s: float = DEFAULT_MAX_AGE_S,
        verify_timeout_s: float = 60.0,
        backoff_max_s: float = 60.0,
        on_close: Callable[[int], None] = lambda conn: None,
        switched: Callable[[Route, int], bool] = lambda route, conn: True,
    ) -> None:
        self.route = route
        self.url = url
        self.sink = sink
        self.log = log
        self.connect = connect
        self.max_age_s = max_age_s
        self.verify_timeout_s = verify_timeout_s
        self.backoff_max_s = backoff_max_s
        self.on_close = on_close
        self.switched = switched
        self.connects = 0
        self.disconnects = 0
        self.rollovers = 0
        self.handle_errors = 0
        self.forced_reconnects = 0
        self.current: _Live | None = None

    @property
    def ping_watch(self) -> PingWatch | None:
        return None if self.current is None else self.current.watch

    @property
    def current_conn(self) -> int | None:
        return None if self.current is None else self.current.conn_id

    async def _open(self) -> _Live:
        conn_id = next(self._ids)
        watch = PingWatch()
        ws = await self.connect(self.url, watch)
        first = asyncio.Event()
        reader = asyncio.create_task(self._read(ws, conn_id, first))
        self.connects += 1
        self.log("ws_connected", route=self.route.value, conn=conn_id)
        return _Live(ws, reader, first, mono_ns(), conn_id, watch)

    async def _read(self, ws: WsLike, conn_id: int, first: asyncio.Event) -> None:
        async for raw in ws:
            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
            first.set()
            try:
                self.sink(Received(text, wall_ms(), mono_ns(), conn_id, self.route))
            except Exception as err:  # one bad message must not drop the connection
                self.handle_errors += 1
                if self.handle_errors <= 10 or self.handle_errors % 1000 == 0:
                    self.log(
                        "handle_error",
                        route=self.route.value,
                        conn=conn_id,
                        count=self.handle_errors,
                        error=repr(err),
                        raw=text[:200],
                    )

    async def _close(self, live: _Live) -> None:
        live.reader.cancel()
        with contextlib.suppress(Exception):  # closing a dead socket must not kill the manager
            await live.ws.close()
        # A reader that already died re-raises its error here (e.g. ConnectionClosedError on an
        # abnormal close); callers log it, so it must not kill the manager (incident 2026-11-02).
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await live.reader
        self.on_close(live.conn_id)

    def force_reconnect(self, reason: str) -> None:
        """Drop the current connection; ``run`` reconnects (used for silent streams)."""
        if self.current is not None and not self.current.reader.done():
            self.forced_reconnects += 1
            self.log("ws_force_reconnect", route=self.route.value, reason=reason)
            self.current.reader.cancel()

    async def run(self) -> None:
        backoff = 1.0
        try:
            while True:
                if self.current is None:
                    try:
                        self.current = await self._open()
                    except Exception as connect_error:  # any connect failure -> backoff
                        self.log(
                            "ws_connect_failed", route=self.route.value, error=repr(connect_error)
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, self.backoff_max_s)
                        continue
                current = self.current
                age_s = (mono_ns() - current.opened_mono_ns) / 1e9
                done, _ = await asyncio.wait(
                    {current.reader}, timeout=max(0.0, self.max_age_s - age_s)
                )
                if current.reader in done:
                    reader = current.reader
                    error = None if reader.cancelled() else reader.exception()
                    lived_s = (mono_ns() - current.opened_mono_ns) / 1e9
                    if lived_s >= STABLE_CONNECTION_S:
                        backoff = 1.0  # a healthy connection ended; reconnect promptly
                    self.disconnects += 1
                    self.log(
                        "ws_disconnected",
                        route=self.route.value,
                        conn=current.conn_id,
                        lived_s=round(lived_s, 1),
                        error=repr(error),
                        backoff_s=backoff,
                    )
                    self.current = None
                    await self._close(current)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self.backoff_max_s)
                    continue
                self.current = await self._rollover(current)
        finally:
            if self.current is not None:
                live, self.current = self.current, None
                await self._close(live)

    async def _rollover(self, old: _Live) -> _Live:
        """Open a successor; close ``old`` once every stream switched (or after a timeout)."""
        self.log("ws_rollover_start", route=self.route.value, old=old.conn_id)
        new: _Live | None = None
        try:
            new = await self._open()
            deadline = mono_ns() + int(self.verify_timeout_s * 1e9)
            while not self.switched(self.route, new.conn_id) and mono_ns() < deadline:
                if new.reader.done() or old.reader.done():
                    break
                await asyncio.sleep(0.05)
            if not new.first_message.is_set() or new.reader.done():
                raise ConnectionError("successor delivered nothing or closed")
        except BaseException as rollover_error:
            if new is not None:
                await self._close(new)
            if not isinstance(rollover_error, Exception):
                raise  # cancellation: successor closed above, propagate
            self.log("ws_rollover_failed", route=self.route.value, error=repr(rollover_error))
            old.opened_mono_ns = mono_ns() - int((self.max_age_s - 60) * 1e9)  # retry in 60 s
            return old
        complete = self.switched(self.route, new.conn_id)
        await self._close(old)  # on_close hands any unswitched stream to the successor
        self.rollovers += 1
        self.log(
            "ws_rollover_done" if complete else "ws_rollover_partial",
            route=self.route.value,
            old=old.conn_id,
            new=new.conn_id,
        )
        return new


class RetryAfterError(Exception):
    def __init__(self, seconds: float, code: int) -> None:
        super().__init__(f"HTTP {code}, retry after {seconds} s")
        self.seconds = seconds


def _http_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "binance-futures-recorder/0.1"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as err:
        if err.code in (418, 429):  # rate limited / IP ban: honour Retry-After
            raise RetryAfterError(float(err.headers.get("Retry-After") or 60), err.code) from err
        raise


@dataclass
class _Pending:
    conn: int
    items: list[tuple[int, Received, dict[str, Any]]] = field(default_factory=list)


class Recorder:
    def __init__(
        self,
        symbols: list[str],
        root: Path,
        *,
        writer: DailyZstdWriter | None = None,
        log: Callable[..., None] | None = None,
        fetch_json: Callable[[str], Any] = _http_json,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self.root = root
        self.writer = writer or DailyZstdWriter(root)
        self.log = log or JsonLog(root)
        self.fetch_json = fetch_json
        self.books = {s: LocalBook(s) for s in self.symbols}
        self.agg_seq = {s: SequenceGap(step=1) for s in self.symbols}
        self.kline_seq = {s: SequenceGap(step=60_000, same_ok=True) for s in self.symbols}
        self.last_key: dict[str, int] = {}
        self.primary: dict[str, int] = {}
        self.pending: dict[str, _Pending] = {}
        self.last_msg_mono: dict[str, int] = {}
        self._force_seen: deque[int] = deque(maxlen=2000)
        self.latency = LatencyStats()
        self.minute = MinuteCounters()
        self.totals: Counter[str] = Counter()
        self.snapshot_inflight: set[str] = set()
        self._snapshot_backoff = dict.fromkeys(self.symbols, 1.0)
        self._snapshot_next_mono = dict.fromkeys(self.symbols, 0)
        self.started_mono_ns = mono_ns()
        self.managers: list[ConnectionManager] = []
        self._background: set[asyncio.Task[None]] = set()
        self.min_free_gb = 20.0
        self.keyed_streams: dict[Route, list[str]] = {
            r: [
                stream_name(s, k)
                for s in self.symbols
                for k in StreamKind
                if k.route is r and k is not StreamKind.FORCE_ORDER
            ]
            for r in Route
        }

    # ----------------------------------------------------------------- records
    def _record(self, ts: int, mono: int, symbol: str, stream: str, body: dict[str, Any]) -> None:
        line = json.dumps({"local_ts_ms": ts, "local_mono_ns": mono, **body}, default=str)
        self.writer.write(ts, symbol, stream, line)

    def event(self, symbol: str, kind: str, **detail: Any) -> None:
        ts, mono = wall_ms(), mono_ns()
        self._record(ts, mono, symbol, EVENTS, {"event": kind, "detail": detail})
        self.totals[f"event:{kind}"] += 1
        self.log(kind, symbol=symbol, **detail)

    def _notes(self, symbol: str, notes: list[BookNote]) -> None:
        for n in notes:
            self.event(symbol, n.kind, reason=n.reason, **n.detail)
            if n.kind == "synced":
                self._snapshot_backoff[symbol] = 1.0

    def _dup(self, kind: StreamKind) -> None:
        self.minute.duplicates[kind.value] += 1
        self.totals["duplicates"] += 1

    # ---------------------------------------------------------------- dispatch
    def handle(self, rec: Received) -> None:
        msg = json.loads(rec.raw)
        stream, data = msg.get("stream"), msg.get("data")
        if stream is None or data is None:
            self.log("ws_non_stream_message", raw=rec.raw[:200])
            return
        symbol, kind = parse_stream(stream)
        key = dedupe_key(kind, data)
        if key is None:  # forceOrder: canonical payload hash, any connection
            h = hash((data.get("E"), json.dumps(data.get("o"), sort_keys=True)))
            if h in self._force_seen:
                self._dup(kind)
                return
            self._force_seen.append(h)
            self._accept(rec, symbol, kind, data)
            return

        primary = self.primary.get(stream)
        if primary is None:
            self.primary[stream] = primary = rec.conn
        if rec.conn < primary:  # a connection being retired: its data is already covered
            self._dup(kind)
            return
        if rec.conn > primary:  # successor: hold until the overlap is proven
            pend = self.pending.get(stream)
            if pend is None or pend.conn != rec.conn:
                pend = self.pending[stream] = _Pending(rec.conn)
            if len(pend.items) < PENDING_MAX:
                pend.items.append((key, rec, data))
            self._try_switch(stream)
            return
        self._accept_keyed(stream, key, rec, symbol, kind, data)
        if stream in self.pending:
            self._try_switch(stream)

    def _accept_keyed(
        self,
        stream: str,
        key: int,
        rec: Received,
        symbol: str,
        kind: StreamKind,
        data: dict[str, Any],
    ) -> None:
        last = self.last_key.get(stream)
        if last is not None and key <= last:
            self._dup(kind)
            return
        self.last_key[stream] = key
        self._accept(rec, symbol, kind, data)

    def _try_switch(self, stream: str, *, force: bool = False) -> None:
        """Hand ``stream`` to the pending successor once the overlap is proven."""
        pend = self.pending.get(stream)
        if pend is None or not pend.items:
            return
        last = self.last_key.get(stream)
        if not force and last is not None and pend.items[0][0] > last:
            return  # successor is ahead of the old connection: wait for old to catch up
        del self.pending[stream]
        self.primary[stream] = pend.conn
        self.totals["stream_switches"] += 1
        symbol, kind = parse_stream(stream)
        for key, rec, data in pend.items:
            self._accept_keyed(stream, key, rec, symbol, kind, data)

    def switched(self, route: Route, conn: int) -> bool:
        """Every keyed stream of ``route`` seen so far is now owned by ``conn``."""
        return all(self.primary[s] == conn for s in self.keyed_streams[route] if s in self.primary)

    def on_connection_closed(self, conn: int) -> None:
        for stream in [s for s, p in self.primary.items() if p == conn]:
            if stream in self.pending:
                self._try_switch(stream, force=True)  # gaps, if any, show in the detectors
            else:
                del self.primary[stream]
        for stream in [s for s, p in self.pending.items() if p.conn == conn]:
            del self.pending[stream]

    def _accept(self, rec: Received, symbol: str, kind: StreamKind, data: dict[str, Any]) -> None:
        line = (
            f'{{"local_ts_ms":{rec.local_ts_ms},"local_mono_ns":{rec.local_mono_ns},'
            f'"conn":{rec.conn},"msg":{rec.raw}}}'
        )
        self.writer.write(rec.local_ts_ms, symbol, kind.value, line)
        self.minute.messages[kind.value] += 1
        self.minute.bytes_raw += len(line)
        self.totals[f"msg:{kind.value}"] += 1
        self.last_msg_mono[stream_name(symbol, kind)] = rec.local_mono_ns
        if "E" in data:
            self.latency.add(kind.value, rec.local_ts_ms, float(data["E"]))

        if kind is StreamKind.DEPTH:
            self._notes(symbol, self.books[symbol].on_event(data))
        elif kind is StreamKind.AGG_TRADE:
            gap = self.agg_seq[symbol].observe(int(data["a"]))
            if gap is not None:
                self.event(symbol, "agg_trade_gap", expected=gap[0], got=gap[1])
        elif kind is StreamKind.KLINE_1M:
            gap = self.kline_seq[symbol].observe(int(data["k"]["t"]))
            if gap is not None:
                self.event(symbol, "kline_gap", expected=gap[0], got=gap[1])

    # --------------------------------------------------------------- snapshots
    async def fetch_and_apply_snapshot(self, symbol: str, reason: str) -> None:
        self.snapshot_inflight.add(symbol)
        retry_after: float | None = None
        try:
            url = f"{REST_BASE}/fapi/v1/depth?symbol={symbol}&limit={SNAPSHOT_LIMIT}"
            snap = await asyncio.wait_for(
                asyncio.to_thread(self.fetch_json, url), SNAPSHOT_TIMEOUT_S
            )
            if self.books[symbol].state is not BookState.NEEDS_SNAPSHOT:
                return  # book changed state meanwhile; never apply to a live book
            ts, mono = wall_ms(), mono_ns()
            self._record(ts, mono, symbol, DEPTH_SNAPSHOT, {"reason": reason, "snapshot": snap})
            self.totals["snapshots"] += 1
            self._notes(symbol, self.books[symbol].apply_snapshot(snap))
        except RetryAfterError as err:
            retry_after = err.seconds
            self.log("snapshot_rate_limited", symbol=symbol, error=str(err))
        except Exception as err:
            self.log("snapshot_failed", symbol=symbol, error=repr(err))
        finally:
            self.snapshot_inflight.discard(symbol)
            if self.books[symbol].state is not BookState.SYNCED:
                wait = self._snapshot_backoff[symbol] if retry_after is None else retry_after
                self._snapshot_next_mono[symbol] = mono_ns() + int(wait * 1e9)
                self._snapshot_backoff[symbol] = min(self._snapshot_backoff[symbol] * 2, 30.0)

    def schedule_snapshots(self) -> None:
        """Fetch a snapshot whenever a book needs one and the stream is already buffering."""
        now = mono_ns()
        for s, book in self.books.items():
            if (
                book.state is BookState.NEEDS_SNAPSHOT
                and book.buffer
                and s not in self.snapshot_inflight
                and now >= self._snapshot_next_mono[s]
            ):
                reason = "initial" if self.totals["snapshots"] < len(self.books) else "resync"
                task = asyncio.create_task(self.fetch_and_apply_snapshot(s, reason))
                self._background.add(task)
                task.add_done_callback(self._background.discard)

    def force_resync(self, reason: str, **detail: Any) -> None:
        for s, book in self.books.items():
            if book.state is not BookState.NEEDS_SNAPSHOT:
                self._notes(s, [book.require_resync(reason, detail)])

    # ---------------------------------------------------------- periodic tasks
    async def _every(self, name: str, every_s: float, fn: Callable[[], Any]) -> None:
        """Run ``fn`` periodically; an error is logged and counted, never fatal."""
        while True:
            await asyncio.sleep(every_s)
            try:
                result = fn()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as err:
                self.totals[f"task_error:{name}"] += 1
                self.log("task_error", task=name, error=repr(err))

    def check_clock(self, detector: JumpDetector) -> None:
        hit = detector.check(time.time() * 1000, mono_ns())
        if hit is not None:
            self.event(GLOBAL_SYMBOL, "clock_jump", cause=hit[0], **hit[1])
            self.force_resync(hit[0], **hit[1])

    def check_stalls(self, now_ns: int | None = None) -> list[str]:
        """Force a reconnect of any route whose always-ticking stream went silent."""
        now = mono_ns() if now_ns is None else now_ns
        stalled: list[str] = []
        for m in self.managers:
            if m.current is None or (now - m.current.opened_mono_ns) / 1e9 < STALL_S:
                continue
            silent = [
                s
                for s in self.keyed_streams[m.route]
                if parse_stream(s)[1] in ALWAYS_TICKING
                and (now - self.last_msg_mono.get(s, m.current.opened_mono_ns)) / 1e9 > STALL_S
            ]
            if silent:
                stalled += silent
                self.event(GLOBAL_SYMBOL, "stream_stalled", route=m.route.value, streams=silent)
                m.force_reconnect("stream_stalled")
        return stalled

    def flush(self) -> None:
        for err in self.writer.flush():
            self.totals["write_errors"] += 1
            self.log("write_error", error=err)

    def book_check(self) -> None:
        for s, book in self.books.items():
            if book.state is BookState.SYNCED:
                bids, asks = book.top(20)
                self._record(
                    wall_ms(),
                    mono_ns(),
                    s,
                    BOOK_CHECK,
                    {
                        "lastUpdateId": book.last_u,
                        "bids": [[str(p), str(q)] for p, q in bids],
                        "asks": [[str(p), str(q)] for p, q in asks],
                    },
                )

    def status(self) -> dict[str, Any]:
        now = mono_ns()

        def since(ns: int | None) -> float | None:
            return None if ns is None else round((now - ns) / 1e9, 1)

        connections = {}
        for m in self.managers:
            w = m.ping_watch
            connections[m.route.value] = {
                "conn": m.current_conn,
                "connects": m.connects,
                "disconnects": m.disconnects,
                "rollovers": m.rollovers,
                "forced_reconnects": m.forced_reconnects,
                "handle_errors": m.handle_errors,
                "server_pings": 0 if w is None else w.pings,
                "max_ping_interval_s": 0.0 if w is None else round(w.max_interval_s, 1),
                "since_last_ping_s": None if w is None else since(w.last_ping_mono_ns),
            }
        return {
            "uptime_s": round((now - self.started_mono_ns) / 1e9, 1),
            "books": {s: b.state.value for s, b in self.books.items()},
            "resyncs": {s: b.resyncs for s, b in self.books.items()},
            "agg_trade_gaps": {s: g.gaps for s, g in self.agg_seq.items()},
            "kline_gaps": {s: g.gaps for s, g in self.kline_seq.items()},
            "since_last_msg_s": {s: since(t) for s, t in sorted(self.last_msg_mono.items())},
            "connections": connections,
            "bytes_written": dict(self.writer.bytes_written),
            "totals": dict(self.totals),
        }

    async def minute_tick(self, min_free_gb: float, healthcheck_url: str | None) -> None:
        counts = self.minute.reset()
        status = self.status()
        self.log("minute", **counts, **status)
        for route, c in status["connections"].items():
            since = c["since_last_ping_s"]
            if since is not None and since > SERVER_PING_EXPECTED_S + 60:
                self.log("server_ping_late", route=route, since_last_ping_s=since)
        free_gb = shutil.disk_usage(self.root).free / 1e9
        if free_gb < min_free_gb:
            self.log("disk_low", free_gb=round(free_gb, 1), threshold_gb=min_free_gb)
        self.write_heartbeat(counts, free_gb)
        if healthcheck_url:
            try:
                await asyncio.to_thread(urllib.request.urlopen, healthcheck_url, None, 10)
            except Exception as err:
                self.log("healthcheck_ping_failed", error=repr(err))

    def write_heartbeat(self, counts: dict[str, object], free_gb: float) -> None:
        path = self.root / "heartbeat.json"
        tmp = path.with_suffix(".tmp")
        body = {
            "ts_ms": wall_ms(),
            "pid": os.getpid(),
            "disk_free_gb": round(free_gb, 1),
            "min_free_gb": self.min_free_gb,
            "last_minute": counts,
            **self.status(),
        }
        tmp.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
        for attempt in range(5):  # another process (AV, indexer, a reader) may hold the file
            try:
                tmp.replace(path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.05)

    async def clock_offset(self) -> None:
        sample = await asyncio.to_thread(measure_offset)
        self.event(
            GLOBAL_SYMBOL,
            "clock_offset",
            offset_ms=round(sample.offset_ms, 1),
            rtt_ms=round(sample.rtt_ms, 1),
        )

    async def hour_tick(self) -> None:
        self.event(GLOBAL_SYMBOL, "latency_hourly", **self.latency.summary_and_reset())
        await self.clock_offset()

    # --------------------------------------------------------------------- run
    async def run(
        self,
        *,
        connect: Connect = websockets_connect,
        flush_s: float = 5.0,
        max_age_s: float = DEFAULT_MAX_AGE_S,
        min_free_gb: float = 20.0,
        healthcheck_url: str | None = None,
        duration_s: float | None = None,
    ) -> None:
        self.min_free_gb = min_free_gb
        self.managers = [
            ConnectionManager(
                r,
                route_url(r, self.symbols),
                self.handle,
                self.log,
                connect=connect,
                max_age_s=max_age_s,
                on_close=self.on_connection_closed,
                switched=self.switched,
            )
            for r in Route
        ]
        self.log("recorder_start", symbols=self.symbols, urls=[m.url for m in self.managers])
        with contextlib.suppress(Exception):
            await self.clock_offset()
        detector = JumpDetector()
        tasks = [asyncio.create_task(m.run()) for m in self.managers]
        tasks += [
            asyncio.create_task(self._every("snapshots", 0.2, self.schedule_snapshots)),
            asyncio.create_task(self._every("clock", 1.0, lambda: self.check_clock(detector))),
            asyncio.create_task(self._every("stalls", 5.0, self.check_stalls)),
            asyncio.create_task(self._every("flush", flush_s, self.flush)),
            asyncio.create_task(self._every("book_check", 300.0, self.book_check)),
            asyncio.create_task(
                self._every("minute", 60.0, lambda: self.minute_tick(min_free_gb, healthcheck_url))
            ),
            asyncio.create_task(self._every("hour", HOUR_S, self.hour_tick)),
        ]
        try:
            if duration_s is None:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(duration_s)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.book_check()
            self.event(GLOBAL_SYMBOL, "latency_final", **self.latency.summary_and_reset())
            self.flush()
            self.log("recorder_stop", **self.status())
            with contextlib.suppress(OSError):
                self.write_heartbeat(self.minute.reset(), shutil.disk_usage(self.root).free / 1e9)
