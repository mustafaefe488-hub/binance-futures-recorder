"""Recorder JSONL.zst -> Parquet schema and verification (no network)."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from binance_futures_recorder.core import Received, Recorder
from binance_futures_recorder.streams import Route
from binance_futures_recorder.to_parquet import (
    complete_days,
    convert_day,
    main,
    out_path,
    verify_day,
)
from tests.test_recorder_core import (  # noqa: F401 - fixed_wall_clock is an autouse fixture
    SNAP,
    TS,
    agg,
    depth,
    fixed_wall_clock,
    kline,
    wrap,
)

DAY = "2026-10-31"
PX = pa.decimal128(18, 8)
TSU = pa.timestamp("ms", tz="UTC")


def book_ticker(u: int) -> str:
    data = {"e": "bookTicker", "u": u, "s": "BTCUSDT", "b": "77259.60", "B": "9.859"}
    return wrap("btcusdt@bookTicker", {**data, "a": "77259.70", "A": "3.180", "T": TS, "E": TS})


def mark_price() -> str:
    data = {"e": "markPriceUpdate", "E": TS, "s": "BTCUSDT", "p": "77259.70000000"}
    extra = {"ap": "77259.7", "P": "77275.00007095", "i": "77262.52130435", "r": "-0.00011496"}
    return wrap("btcusdt@markPrice@1s", {**data, **extra, "T": TS + 3_600_000})


def force_order() -> str:
    o = {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.014", "p": "9910"}
    o |= {"ap": "9910", "X": "FILLED", "l": "0.014", "z": "0.014", "T": TS}
    return wrap("btcusdt@forceOrder", {"e": "forceOrder", "E": TS, "o": o})


@pytest.fixture
async def recorded(tmp_path: Path) -> Path:
    r = Recorder(["BTCUSDT"], tmp_path, log=lambda *a, **k: None, fetch_json=lambda url: SNAP)
    raws = [depth(95, 103, 94), agg(1), agg(2), book_ticker(5), book_ticker(6), mark_price()]
    raws += [force_order(), kline(TS - 56_000, TS)]
    for i, raw in enumerate(raws):
        r.handle(Received(raw, TS + i, 1000 + i, 1, Route.PUBLIC))
    await r.fetch_and_apply_snapshot("BTCUSDT", "initial")
    r.book_check()
    r.writer.flush()
    return tmp_path


def read(root: Path, stream: str, symbol: str = "BTCUSDT") -> pa.Table:
    return pq.read_table(out_path(root, DAY, symbol, stream))


async def test_all_streams_convert_with_typed_schema(recorded: Path) -> None:
    results = {(r.symbol, r.stream): r for r in convert_day(recorded, DAY)}
    assert results[("BTCUSDT", "aggTrade")].rows == 2
    assert results[("BTCUSDT", "bookTicker")].rows == 2
    assert {s for _, s in results} >= {
        "aggTrade", "bookTicker", "markPrice", "forceOrder", "kline_1m", "depth",
        "depthSnapshot", "bookCheck", "events",
    }  # fmt: skip
    for (symbol, stream), _ in results.items():
        tbl = read(recorded, stream, symbol)
        assert not any(pa.types.is_floating(f.type) for f in tbl.schema), stream
        assert tbl.schema.field("local_ts").type == TSU

    bt = read(recorded, "bookTicker").to_pylist()
    assert bt[0]["best_bid_price"] == Decimal("77259.60")
    assert bt[0]["event_time"] == datetime.fromtimestamp(TS / 1000, UTC)
    assert bt[0]["local_mono_ns"] == 1003

    mp = read(recorded, "markPrice")
    assert mp.schema.field("funding_rate").type == PX
    assert mp.column("funding_rate")[0].as_py() == Decimal("-0.00011496")

    d = read(recorded, "depth")
    assert d.schema.field("bids").type == pa.list_(pa.struct([("price", PX), ("qty", PX)]))
    assert d.column("bids")[0].as_py() == [{"price": Decimal("99.0"), "qty": Decimal("1")}]
    assert d.column("prev_final_update_id")[0].as_py() == 94

    snap = read(recorded, "depthSnapshot").to_pylist()[0]
    assert snap["reason"] == "initial"
    assert snap["last_update_id"] == 100

    k = read(recorded, "kline_1m")
    assert k.schema.field("quote_volume").type == pa.decimal128(24, 8)
    ev = read(recorded, "events").to_pylist()
    assert ev[0]["event"] == "synced"
    assert json.loads(ev[0]["detail"])["lastUpdateId"] == 100

    fo = read(recorded, "forceOrder").to_pylist()[0]
    assert (fo["side"], fo["orig_qty"]) == ("SELL", Decimal("0.014"))


async def test_idempotent_and_verified_raw_deletion(recorded: Path) -> None:
    convert_day(recorded, DAY)
    again = convert_day(recorded, DAY)
    assert all(r.skipped for r in again)
    deleted = convert_day(recorded, DAY, delete_raw=True)
    assert all(r.raw_deleted for r in deleted)
    assert not (recorded / "raw" / DAY).exists()
    assert read(recorded, "aggTrade").num_rows == 2


def test_complete_days_and_cli_guard(tmp_path: Path) -> None:
    for d in ("2026-10-30", "2026-10-31"):
        (tmp_path / "raw" / d).mkdir(parents=True)
    assert complete_days(tmp_path, "2026-10-31") == ["2026-10-30"]
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    with pytest.raises(SystemExit):
        main(["--root", str(tmp_path), "--date", today, "--delete-raw"])


async def test_inventory_lists_files_with_row_counts(
    recorded: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    convert_day(recorded, DAY)
    assert main(["--root", str(recorded), "--inventory"]) == 0
    lines = dict(line.split("\t") for line in capsys.readouterr().out.splitlines())
    assert lines[f"aggTrade/BTCUSDT/{DAY}.parquet"] == "2"
    assert lines[f"bookTicker/BTCUSDT/{DAY}.parquet"] == "2"


async def test_verify_day_counts_chain_breaks(
    recorded: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    convert_day(recorded, DAY)
    report = verify_day(recorded, DAY)
    assert report["BTCUSDT"] == {
        "depth_rows": 1, "pu_breaks": 0, "logged_resyncs": 0, "agg_id_gaps": 0
    }  # fmt: skip
    assert main(["--root", str(recorded), "--verify", DAY]) == 0
    assert capsys.readouterr().out.strip().endswith("OK")
    assert main(["--root", str(recorded), "--verify", "2000-01-01"]) == 1  # no data -> CHECK


def test_batches_are_bounded_by_bytes_and_rows(tmp_path: Path) -> None:
    from binance_futures_recorder.to_parquet import _batches  # noqa: PLC0415
    from binance_futures_recorder.writer import DailyZstdWriter, raw_path  # noqa: PLC0415

    w = DailyZstdWriter(tmp_path)
    big = json.dumps({"n": 0, "pad": "x" * 1000})  # ~1 KB per line, like a depth event
    for _ in range(50):
        w.write(TS, "BTCUSDT", "depth", big)
    w.flush()
    path = raw_path(tmp_path, DAY, "BTCUSDT", "depth")
    by_bytes = [len(b) for b in _batches(path, max_rows=1000, max_bytes=10_000)]
    assert sum(by_bytes) == 50
    assert max(by_bytes) == 10  # 10 lines of ~1 KB reach the 10 KB budget
    by_rows = [len(b) for b in _batches(path, max_rows=7, max_bytes=1 << 30)]
    assert by_rows == [7] * 7 + [1]
