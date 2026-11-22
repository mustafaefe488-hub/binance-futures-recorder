"""LocalBook against the Binance USD-M local order book rules (no network)."""

from decimal import Decimal
from typing import Any

from binance_futures_recorder.book import BookState, LocalBook


def ev(
    first: int, last: int, prev: int, b: list[list[str]] | None = None, a=None
) -> dict[str, Any]:
    return {"e": "depthUpdate", "U": first, "u": last, "pu": prev, "b": b or [], "a": a or []}


SNAP = {
    "lastUpdateId": 100,
    "bids": [["99.0", "1.0"], ["98.0", "2.0"]],
    "asks": [["101.0", "1.5"], ["102.0", "3.0"]],
}


def synced_book() -> LocalBook:
    book = LocalBook("BTCUSDT")
    book.on_event(ev(95, 103, 94))
    notes = book.apply_snapshot(SNAP)
    assert [n.kind for n in notes] == ["synced"]
    return book


def test_buffers_until_snapshot() -> None:
    book = LocalBook("BTCUSDT")
    assert book.on_event(ev(90, 95, 89)) == []
    assert book.state is BookState.NEEDS_SNAPSHOT
    assert len(book.buffer) == 1


def test_drops_old_events_and_bridges_with_u_le_last_le_u() -> None:
    book = LocalBook("BTCUSDT")
    book.on_event(ev(80, 90, 79))  # u < lastUpdateId -> dropped
    book.on_event(ev(91, 99, 90))  # u < lastUpdateId -> dropped
    book.on_event(ev(100, 104, 99, b=[["99.0", "5.0"]]))  # U <= 100 <= u -> first processed
    book.on_event(ev(105, 110, 104, a=[["101.0", "0"]]))  # pu == previous u
    notes = book.apply_snapshot(SNAP)
    assert [n.kind for n in notes] == ["synced"]
    assert notes[0].detail == {"lastUpdateId": 100, "U": 100, "u": 104}
    assert book.state is BookState.SYNCED
    assert book.last_u == 110
    assert book.bids[Decimal("99.0")] == Decimal("5.0")
    assert Decimal("101.0") not in book.asks  # quantity 0 removes the level


def test_snapshot_without_buffer_bridges_on_next_live_event() -> None:
    book = LocalBook("BTCUSDT")
    assert book.apply_snapshot(SNAP) == []
    assert book.state is BookState.SYNCING
    assert book.on_event(ev(50, 99, 49)) == []  # still older than the snapshot
    notes = book.on_event(ev(99, 101, 98))
    assert [n.kind for n in notes] == ["synced"]


def test_stale_snapshot_when_first_event_starts_after_it() -> None:
    book = LocalBook("BTCUSDT")
    book.on_event(ev(105, 110, 104))  # U > lastUpdateId: updates 101..104 are missing
    book.on_event(ev(111, 115, 110))
    notes = book.apply_snapshot(SNAP)
    assert [n.kind for n in notes] == ["snapshot_stale"]
    assert book.state is BookState.NEEDS_SNAPSHOT
    assert [e["u"] for e in book.buffer] == [110, 115]  # nothing buffered is lost


def test_intact_pu_chain_never_resyncs() -> None:
    book = synced_book()
    u = book.last_u
    assert u is not None
    for _ in range(100):
        assert book.on_event(ev(u + 1, u + 5, u)) == []
        u += 5
    assert book.resyncs == 0
    assert book.events_applied == 101


def test_broken_pu_chain_resyncs_and_keeps_event() -> None:
    book = synced_book()
    notes = book.on_event(ev(120, 125, 119))  # previous u was 103
    assert [(n.kind, n.reason) for n in notes] == [("resync", "pu_mismatch")]
    assert notes[0].detail["expected_pu"] == 103
    assert book.state is BookState.NEEDS_SNAPSHOT
    assert book.bids == {}
    assert [e["u"] for e in book.buffer] == [125]
    # the next snapshot bridges from the buffered event
    notes = book.apply_snapshot({**SNAP, "lastUpdateId": 121})
    assert [n.kind for n in notes] == ["synced"]


def test_restart_during_replay_keeps_newer_buffered_events() -> None:
    book = LocalBook("BTCUSDT")
    for e in (ev(100, 103, 99), ev(104, 106, 103), ev(200, 205, 150), ev(206, 210, 205)):
        book.on_event(e)
    notes = book.apply_snapshot(SNAP)
    assert [n.kind for n in notes] == ["synced", "resync"]
    assert [e["u"] for e in book.buffer] == [205, 210]


def test_crossed_book_forces_resync() -> None:
    book = synced_book()
    notes = book.on_event(ev(104, 106, 103, b=[["105.0", "1.0"]]))  # bid above best ask
    assert [(n.kind, n.reason) for n in notes] == [("resync", "crossed_book")]


def test_removing_unknown_level_is_fine_and_top_is_sorted() -> None:
    book = synced_book()
    book.on_event(ev(104, 106, 103, b=[["50.0", "0"], ["99.5", "4"]]))
    bids, asks = book.top(2)
    assert bids == [(Decimal("99.5"), Decimal("4")), (Decimal("99.0"), Decimal("1.0"))]
    assert asks[0] == (Decimal("101.0"), Decimal("1.5"))


def test_forced_resync_for_clock_jump() -> None:
    book = synced_book()
    note = book.require_resync("wall_clock_jump", {"d_wall_ms": 5000})
    assert (note.kind, note.reason) == ("resync", "wall_clock_jump")
    assert book.state is BookState.NEEDS_SNAPSHOT
