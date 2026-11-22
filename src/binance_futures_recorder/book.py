"""Local order book maintained exactly as Binance documents for USD-M diff depth.

Pinned: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
1. Open the diff depth stream and buffer events.
2. Get a snapshot: GET /fapi/v1/depth?symbol=...&limit=1000.
3. Drop any event with ``u < lastUpdateId``.
4. The first processed event must have ``U <= lastUpdateId <= u``.
5. Every later event must have ``pu == previous u``; otherwise restart from step 2.
6. Quantities are absolute; quantity 0 removes the level (removing an unknown level is normal).

The recorder stores the raw diff events and the snapshots; this book exists to prove the
``pu`` chain is intact and to write periodic top-20 check snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any


class BookState(StrEnum):
    NEEDS_SNAPSHOT = "needs_snapshot"  # buffering, a snapshot must be fetched
    SYNCING = "syncing"  # snapshot applied, waiting for the bridging event
    SYNCED = "synced"


@dataclass(frozen=True, slots=True)
class BookNote:
    """Something the recorder must log (and usually act on)."""

    kind: str  # "synced" | "resync" | "snapshot_stale"
    reason: str
    detail: dict[str, Any]


Level = tuple[Decimal, Decimal]


class LocalBook:
    MAX_BUFFER = 10_000  # ~16 minutes of 100 ms events; beyond that, start over

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.state = BookState.NEEDS_SNAPSHOT
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.snapshot_id: int | None = None
        self.last_u: int | None = None
        self.buffer: list[dict[str, Any]] = []
        self.resyncs = 0
        self.events_applied = 0

    # ------------------------------------------------------------------ events
    def on_event(self, ev: dict[str, Any]) -> list[BookNote]:
        if self.state is BookState.NEEDS_SNAPSHOT:
            self.buffer.append(ev)
            if len(self.buffer) > self.MAX_BUFFER:
                del self.buffer[: len(self.buffer) - self.MAX_BUFFER]
            return []
        if self.state is BookState.SYNCING:
            return self._bridge(ev)
        if int(ev["pu"]) != self.last_u:
            return [
                self._restart(
                    "pu_mismatch", {"expected_pu": self.last_u, "pu": ev["pu"], "u": ev["u"]}, ev
                )
            ]
        self._apply(ev)
        return self._crossed_check()

    def apply_snapshot(self, snap: dict[str, Any]) -> list[BookNote]:
        """Apply a REST snapshot, then replay buffered events per rules 3-5."""
        self.bids = {Decimal(p): Decimal(q) for p, q in snap["bids"]}
        self.asks = {Decimal(p): Decimal(q) for p, q in snap["asks"]}
        self.snapshot_id = int(snap["lastUpdateId"])
        self.last_u = None
        self.state = BookState.SYNCING
        buffered, self.buffer = self.buffer, []
        notes: list[BookNote] = []
        for i, ev in enumerate(buffered):
            notes += self.on_event(ev)
            if self.state is BookState.NEEDS_SNAPSHOT:
                # restart buffered this event; keep the newer ones for the next snapshot
                self.buffer.extend(buffered[i + 1 :])
                break
        return notes

    def require_resync(self, reason: str, detail: dict[str, Any] | None = None) -> BookNote:
        """Force a rebuild from a fresh snapshot (e.g. after a clock jump or reconnect)."""
        return self._restart(reason, detail or {}, None)

    # ----------------------------------------------------------------- helpers
    def _bridge(self, ev: dict[str, Any]) -> list[BookNote]:
        assert self.snapshot_id is not None
        first, last = int(ev["U"]), int(ev["u"])
        if last < self.snapshot_id:
            return []  # rule 3: older than the snapshot
        if first <= self.snapshot_id <= last:  # rule 4
            self._apply(ev)
            self.state = BookState.SYNCED
            note = BookNote(
                "synced", "bridged", {"lastUpdateId": self.snapshot_id, "U": first, "u": last}
            )
            return [note, *self._crossed_check()]
        # first > snapshot_id: events between snapshot and stream are missing
        self.resyncs += 1
        self.state = BookState.NEEDS_SNAPSHOT
        self.buffer = [ev]
        return [
            BookNote(
                "snapshot_stale",
                "first_event_after_snapshot",
                {"lastUpdateId": self.snapshot_id, "U": first, "u": last},
            )
        ]

    def _apply(self, ev: dict[str, Any]) -> None:
        for side, levels in ((self.bids, ev["b"]), (self.asks, ev["a"])):
            for p, q in levels:
                price, qty = Decimal(p), Decimal(q)
                if qty == 0:
                    side.pop(price, None)
                else:
                    side[price] = qty
        self.last_u = int(ev["u"])
        self.events_applied += 1

    def _crossed_check(self) -> list[BookNote]:
        if self.bids and self.asks:
            best_bid, best_ask = max(self.bids), min(self.asks)
            if best_bid >= best_ask:
                return [
                    self._restart(
                        "crossed_book", {"best_bid": str(best_bid), "best_ask": str(best_ask)}, None
                    )
                ]
        return []

    def _restart(self, reason: str, detail: dict[str, Any], ev: dict[str, Any] | None) -> BookNote:
        self.resyncs += 1
        self.state = BookState.NEEDS_SNAPSHOT
        self.bids, self.asks = {}, {}
        self.snapshot_id = None
        self.last_u = None
        self.buffer = [ev] if ev is not None else []
        return BookNote("resync", reason, detail)

    def top(self, n: int = 20) -> tuple[list[Level], list[Level]]:
        bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        return bids, asks
