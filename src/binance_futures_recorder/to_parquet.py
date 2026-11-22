"""Convert recorder JSONL.zst days to typed Parquet.

  uv run python -m binance_futures_recorder.to_parquet                  # every complete UTC day
  uv run python -m binance_futures_recorder.to_parquet --date 2026-10-31 --delete-raw

Input:  data/live/raw/{YYYY-MM-DD}/{SYMBOL}_{stream}.jsonl.zst
Output: data/live/parquet/{stream}/{SYMBOL}/{YYYY-MM-DD}.parquet (zstd)
Types: timestamps ``timestamp[ms, UTC]`` (epoch unit checked),
prices/quantities ``decimal128(18, 8)``, notional ``decimal128(24, 8)``, ids int64; no float.
Book levels are ``list<struct<price, qty>>``. A day is converted only when complete (before
today, UTC) unless named explicitly; ``--delete-raw`` removes a raw file only after its Parquet
was read back with the same row count as the raw file's line count.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from binance_futures_recorder._shared import INT, PX, QV, TS, to_epoch_ms
from binance_futures_recorder.writer import LIVE_ROOT, read_lines

# A batch ends at whichever limit comes first. Rows differ by ~100x in size (a depth event
# carries dozens to hundreds of levels, a bookTicker a handful of fields); 200k depth rows
# exceeded a 1.5 GB memory limit on a small server. ~16 MB of JSON per batch keeps
# the Python objects of one batch in the low hundreds of MB.
BATCH_ROWS = 200_000
BATCH_BYTES = 16 << 20
STR = pa.string()
BOOL = pa.bool_()
LEVEL = pa.struct([("price", PX), ("qty", PX)])
LEVELS = pa.list_(LEVEL)
META_SOURCE_BYTES = b"recorder.source_bytes"
META_LINES = b"recorder.source_lines"

Row = dict[str, Any]
Getter = Callable[[Row], Any]


def _d(r: Row) -> Row:
    data: Row = r["msg"]["data"]
    return data


def _common() -> list[tuple[str, pa.DataType, Getter]]:
    return [
        ("local_ts", TS, lambda r: r["local_ts_ms"]),
        ("local_mono_ns", INT, lambda r: r["local_mono_ns"]),
        ("conn", INT, lambda r: r["conn"]),
        ("event_time", TS, lambda r: _d(r)["E"]),
    ]


def _own() -> list[tuple[str, pa.DataType, Getter]]:
    return [
        ("local_ts", TS, lambda r: r["local_ts_ms"]),
        ("local_mono_ns", INT, lambda r: r["local_mono_ns"]),
    ]


COLUMNS: dict[str, list[tuple[str, pa.DataType, Getter]]] = {
    "aggTrade": [
        *_common(),
        ("agg_trade_id", INT, lambda r: _d(r)["a"]),
        ("price", PX, lambda r: _d(r)["p"]),
        ("quantity", PX, lambda r: _d(r)["q"]),
        ("normal_quantity", PX, lambda r: _d(r).get("nq")),
        ("first_trade_id", INT, lambda r: _d(r)["f"]),
        ("last_trade_id", INT, lambda r: _d(r)["l"]),
        ("trade_time", TS, lambda r: _d(r)["T"]),
        ("is_buyer_maker", BOOL, lambda r: _d(r)["m"]),
    ],
    "bookTicker": [
        *_common(),
        ("update_id", INT, lambda r: _d(r)["u"]),
        ("transaction_time", TS, lambda r: _d(r)["T"]),
        ("best_bid_price", PX, lambda r: _d(r)["b"]),
        ("best_bid_qty", PX, lambda r: _d(r)["B"]),
        ("best_ask_price", PX, lambda r: _d(r)["a"]),
        ("best_ask_qty", PX, lambda r: _d(r)["A"]),
    ],
    "markPrice": [
        *_common(),
        ("mark_price", PX, lambda r: _d(r)["p"]),
        ("index_price", PX, lambda r: _d(r)["i"]),
        ("est_settle_price", PX, lambda r: _d(r)["P"]),
        ("funding_rate", PX, lambda r: _d(r)["r"]),
        ("mark_price_avg", PX, lambda r: _d(r).get("ap")),
        ("next_funding_time", TS, lambda r: _d(r)["T"]),
    ],
    "forceOrder": [
        *_common(),
        ("side", STR, lambda r: _d(r)["o"]["S"]),
        ("order_type", STR, lambda r: _d(r)["o"]["o"]),
        ("time_in_force", STR, lambda r: _d(r)["o"]["f"]),
        ("orig_qty", PX, lambda r: _d(r)["o"]["q"]),
        ("price", PX, lambda r: _d(r)["o"]["p"]),
        ("avg_price", PX, lambda r: _d(r)["o"]["ap"]),
        ("status", STR, lambda r: _d(r)["o"]["X"]),
        ("last_filled_qty", PX, lambda r: _d(r)["o"]["l"]),
        ("filled_acc_qty", PX, lambda r: _d(r)["o"]["z"]),
        ("trade_time", TS, lambda r: _d(r)["o"]["T"]),
    ],
    "kline_1m": [
        *_common(),
        ("open_time", TS, lambda r: _d(r)["k"]["t"]),
        ("close_time", TS, lambda r: _d(r)["k"]["T"]),
        ("first_trade_id", INT, lambda r: _d(r)["k"]["f"]),
        ("last_trade_id", INT, lambda r: _d(r)["k"]["L"]),
        ("open", PX, lambda r: _d(r)["k"]["o"]),
        ("high", PX, lambda r: _d(r)["k"]["h"]),
        ("low", PX, lambda r: _d(r)["k"]["l"]),
        ("close", PX, lambda r: _d(r)["k"]["c"]),
        ("volume", PX, lambda r: _d(r)["k"]["v"]),
        ("trades", INT, lambda r: _d(r)["k"]["n"]),
        ("closed", BOOL, lambda r: _d(r)["k"]["x"]),
        ("quote_volume", QV, lambda r: _d(r)["k"]["q"]),
        ("taker_buy_volume", PX, lambda r: _d(r)["k"]["V"]),
        ("taker_buy_quote_volume", QV, lambda r: _d(r)["k"]["Q"]),
    ],
    "depth": [
        *_common(),
        ("transaction_time", TS, lambda r: _d(r)["T"]),
        ("first_update_id", INT, lambda r: _d(r)["U"]),
        ("final_update_id", INT, lambda r: _d(r)["u"]),
        ("prev_final_update_id", INT, lambda r: _d(r)["pu"]),
        ("bids", LEVELS, lambda r: _d(r)["b"]),
        ("asks", LEVELS, lambda r: _d(r)["a"]),
    ],
    "depthSnapshot": [
        *_own(),
        ("reason", STR, lambda r: r["reason"]),
        ("last_update_id", INT, lambda r: r["snapshot"]["lastUpdateId"]),
        ("event_time", TS, lambda r: r["snapshot"]["E"]),
        ("transaction_time", TS, lambda r: r["snapshot"]["T"]),
        ("bids", LEVELS, lambda r: r["snapshot"]["bids"]),
        ("asks", LEVELS, lambda r: r["snapshot"]["asks"]),
    ],
    "bookCheck": [
        *_own(),
        ("last_update_id", INT, lambda r: r["lastUpdateId"]),
        ("bids", LEVELS, lambda r: r["bids"]),
        ("asks", LEVELS, lambda r: r["asks"]),
    ],
    "events": [
        *_own(),
        ("event", STR, lambda r: r["event"]),
        ("detail", STR, lambda r: json.dumps(r.get("detail", {}), sort_keys=True)),
    ],
}

KEY_COLUMN = {"aggTrade": "agg_trade_id", "bookTicker": "update_id", "depth": "final_update_id"}


def schema_for(stream: str) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t, _ in COLUMNS[stream]] + [("symbol", STR)])


def _levels(values: list[list[list[str]]]) -> pa.Array:
    offsets = [0]
    prices: list[str] = []
    qtys: list[str] = []
    for levels in values:
        for p, q in levels:
            prices.append(p)
            qtys.append(q)
        offsets.append(len(prices))
    struct = pa.StructArray.from_arrays(
        [pc.cast(pa.array(prices, STR), PX), pc.cast(pa.array(qtys, STR), PX)],
        fields=list(LEVEL),
    )
    return pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), struct).cast(LEVELS)


def _column(typ: pa.DataType, values: list[Any]) -> pa.Array:
    if typ == TS:
        return to_epoch_ms(pa.array(values, INT)).cast(TS)
    if pa.types.is_decimal(typ):
        return pc.cast(pa.array(values, STR), typ)
    if typ == LEVELS:
        return _levels(values)
    return pa.array(values, typ)


def build_table(stream: str, symbol: str, rows: list[Row]) -> pa.Table:
    cols = {name: _column(typ, [get(r) for r in rows]) for name, typ, get in COLUMNS[stream]}
    cols["symbol"] = pa.array([symbol] * len(rows), STR)
    return pa.table(cols, schema=schema_for(stream))


def _batches(
    path: Path, max_rows: int = BATCH_ROWS, max_bytes: int = BATCH_BYTES
) -> Iterator[list[Row]]:
    batch: list[Row] = []
    size = 0
    for line in read_lines(path):
        batch.append(json.loads(line))
        size += len(line)
        if len(batch) >= max_rows or size >= max_bytes:
            yield batch
            batch, size = [], 0
    if batch:
        yield batch


@dataclass
class FileResult:
    stream: str
    symbol: str
    rows: int
    key_decreases: int
    skipped: bool
    raw_deleted: bool = False


def split_name(path: Path) -> tuple[str, str]:
    """``BTCUSDT_bookTicker.jsonl.zst`` -> ("BTCUSDT", "bookTicker")."""
    symbol, _, stream = path.name.removesuffix(".jsonl.zst").partition("_")
    return symbol, stream


def out_path(root: Path, day: str, symbol: str, stream: str) -> Path:
    return root / "parquet" / stream / symbol / f"{day}.parquet"


def convert_file(raw: Path, root: Path, day: str, *, force: bool = False) -> FileResult:
    symbol, stream = split_name(raw)
    out = out_path(root, day, symbol, stream)
    size = str(raw.stat().st_size).encode()
    if out.is_file() and not force:
        meta = pq.read_schema(out).metadata or {}
        if meta.get(META_SOURCE_BYTES) == size:
            return FileResult(stream, symbol, pq.read_metadata(out).num_rows, 0, skipped=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".part")
    rows = lines = decreases = 0
    last_key: int | None = None
    key_col = KEY_COLUMN.get(stream)
    try:
        writer: pq.ParquetWriter | None = None
        for batch in _batches(raw):
            lines += len(batch)
            tbl = build_table(stream, symbol, batch)
            if key_col is not None:
                keys = tbl.column(key_col).to_pylist()
                for k in keys:
                    if last_key is not None and k <= last_key:
                        decreases += 1
                    last_key = k
            if writer is None:
                meta = {META_SOURCE_BYTES: size}
                writer = pq.ParquetWriter(tmp, tbl.schema.with_metadata(meta), compression="zstd")
            writer.write_table(tbl)
            rows += tbl.num_rows
        if writer is None:
            writer = pq.ParquetWriter(
                tmp, schema_for(stream).with_metadata({META_SOURCE_BYTES: size}), compression="zstd"
            )
        writer.close()
        tmp.replace(out)
    finally:
        tmp.unlink(missing_ok=True)
    if rows != lines:
        raise ValueError(f"{raw}: {lines} lines but {rows} rows written")
    return FileResult(stream, symbol, rows, decreases, skipped=False)


def verified(raw: Path, out: Path) -> bool:
    """Parquet row count equals the raw file's line count."""
    rows: int = pq.read_metadata(out).num_rows
    return rows == sum(1 for _ in read_lines(raw))


def convert_day(
    root: Path, day: str, *, force: bool = False, delete_raw: bool = False
) -> list[FileResult]:
    results = []
    for raw in sorted((root / "raw" / day).glob("*.jsonl.zst")):
        res = convert_file(raw, root, day, force=force)
        if delete_raw and verified(raw, out_path(root, day, res.symbol, res.stream)):
            raw.unlink()
            res.raw_deleted = True
        results.append(res)
    day_dir = root / "raw" / day
    if delete_raw and day_dir.is_dir() and not any(day_dir.iterdir()):
        day_dir.rmdir()
    return results


def complete_days(root: Path, today: str) -> list[str]:
    raw = root / "raw"
    if not raw.is_dir():
        return []
    return sorted(d.name for d in raw.iterdir() if d.is_dir() and d.name < today)


def inventory(root: Path) -> list[tuple[str, int]]:
    """(path relative to {root}/parquet in POSIX form, row count) for every Parquet file."""
    base = root / "parquet"
    if not base.is_dir():
        return []
    return [
        (f.relative_to(base).as_posix(), int(pq.read_metadata(f).num_rows))
        for f in sorted(base.rglob("*.parquet"))
    ]


def _breaks(values: list[int], previous: list[int] | None = None, step: int = 1) -> int:
    """Count positions where a sequence does not continue (``prev`` given: pu vs previous u)."""
    if previous is not None:
        return sum(1 for i in range(1, len(values)) if previous[i] != values[i - 1])
    return sum(1 for i in range(1, len(values)) if values[i] != values[i - 1] + step)


def verify_day(root: Path, day: str) -> dict[str, dict[str, int]]:
    """Per symbol: depth rows, pu-chain breaks, logged resyncs, aggTrade id gaps for ``day``."""
    out: dict[str, dict[str, int]] = {}
    for f in sorted((root / "parquet" / "depth").glob(f"*/{day}.parquet")):
        symbol = f.parent.name
        t = pq.read_table(f, columns=["final_update_id", "prev_final_update_id"])
        u = t.column("final_update_id").to_pylist()
        pu = t.column("prev_final_update_id").to_pylist()
        res = {"depth_rows": len(u), "pu_breaks": _breaks(u, previous=pu)}
        ev = root / "parquet" / "events" / symbol / f"{day}.parquet"
        events = (
            pq.read_table(ev, columns=["event"]).column("event").to_pylist() if ev.is_file() else []
        )
        res["logged_resyncs"] = sum(1 for e in events if e in ("resync", "snapshot_stale"))
        agg = root / "parquet" / "aggTrade" / symbol / f"{day}.parquet"
        if agg.is_file():
            ids = pq.read_table(agg, columns=["agg_trade_id"]).column("agg_trade_id").to_pylist()
            res["agg_id_gaps"] = _breaks(ids)
        out[symbol] = res
    return out


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m binance_futures_recorder.to_parquet")
    p.add_argument("--root", type=Path, default=LIVE_ROOT)
    p.add_argument("--date", help="YYYY-MM-DD (default: every complete UTC day)")
    p.add_argument("--force", action="store_true")
    p.add_argument("--delete-raw", action="store_true", help="delete raw files once verified")
    p.add_argument(
        "--inventory", action="store_true", help="print '<file>\t<rows>' for every Parquet file"
    )
    p.add_argument("--verify", metavar="YYYY-MM-DD", help="chain/gap report of a converted day")
    args = p.parse_args(argv)
    if args.verify:
        report = verify_day(args.root, args.verify)
        for symbol, res in report.items():
            print(f"{args.verify} {symbol} " + " ".join(f"{k}={v}" for k, v in res.items()))
        ok = bool(report) and all(r["pu_breaks"] <= r["logged_resyncs"] for r in report.values())
        print("OK" if ok else "CHECK: pu breaks without a logged resync, or no depth data")
        return 0 if ok else 1
    if args.inventory:
        for rel, rows in inventory(args.root):
            print(f"{rel}	{rows}")
        return 0
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    days = [args.date] if args.date else complete_days(args.root, today)
    if args.date == today and args.delete_raw:
        p.error("refusing --delete-raw for today: the recorder is still appending")
    for day in days:
        for r in convert_day(args.root, day, force=args.force, delete_raw=args.delete_raw):
            flags = (" (skipped)" if r.skipped else "") + (" raw deleted" if r.raw_deleted else "")
            print(
                f"{day} {r.symbol:8} {r.stream:14} rows={r.rows:>11,} "
                f"key_decreases={r.key_decreases}{flags}"
            )
    if not days:
        print("no complete days to convert")
    return 0


if __name__ == "__main__":
    sys.exit(main())
