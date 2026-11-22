# binance-futures-recorder

A live market-data recorder for Binance USD-M perpetual futures. It keeps WebSocket
connections open, writes every message it receives to daily zstd-compressed JSON-lines files,
maintains a local order book to prove the depth stream is complete, and converts finished
days to typed Parquet.

It needs no API key: it reads public market data only and places no orders.

## What it records

Per symbol (the CLI accepts `BTCUSDT` and `ETHUSDT`):

| Stream | Binance stream name | Route |
|---|---|---|
| Diff depth | `<symbol>@depth@100ms` | `/public` |
| Best bid/ask | `<symbol>@bookTicker` | `/public` |
| Aggregate trades | `<symbol>@aggTrade` | `/market` |
| Mark price, index, funding | `<symbol>@markPrice@1s` | `/market` |
| Liquidations | `<symbol>@forceOrder` | `/market` |
| 1-minute klines | `<symbol>@kline_1m` | `/market` |

Binance serves high-frequency public data (depth, bookTicker) only on
`wss://fstream.binance.com/public` and the other market streams only on
`wss://fstream.binance.com/market`, so the recorder keeps one combined-stream connection per
route.

Each raw line carries the local receive time (`local_ts_ms`, wall clock), a monotonic
timestamp (`local_mono_ns`), the connection id (`conn`) and the original message (`msg`).

The recorder also writes records of its own:

- `depthSnapshot`: every REST snapshot (`GET /fapi/v1/depth?limit=1000`) used to build the book.
- `bookCheck`: the top 20 levels of each synced local book, every 5 minutes and on shutdown.
- `events`: book syncs and resyncs, aggTrade id gaps, kline gaps, clock offset measurements
  (`GET /fapi/v1/time`, hourly), clock jumps and process stalls, stalled streams, and hourly
  latency percentiles (receive time minus exchange event time). Events that are not tied to
  a symbol use the symbol name `ALL`.

## How the local order book is kept honest

The book follows Binance's documented procedure for a local USD-M order book:

1. Diff depth events are buffered until a snapshot is available.
2. A REST snapshot is fetched (rate limits honour `Retry-After`; failures back off).
3. Events with `u < lastUpdateId` are dropped.
4. The first applied event must satisfy `U <= lastUpdateId <= u`. If the first buffered event
   already starts after the snapshot, the snapshot is stale and a new one is fetched.
5. Every later event must have `pu` equal to the previous event's `u`. A mismatch logs a
   `resync` event and the book is rebuilt from a fresh snapshot.
6. A crossed book (best bid >= best ask) also forces a resync.

A resync is also forced after a wall-clock jump or a process stall (for example after the
machine slept). The raw diff events are always written, whatever the book state.

Other checks:

- aggTrade ids and kline open times are watched for gaps; every gap is logged as an event.
- A stream that should tick continuously (depth, bookTicker, markPrice) and stays silent for
  30 seconds forces a reconnect of its route.
- Connections are rolled over before Binance's 24-hour limit (default after 23 hours). The
  successor connection runs in parallel and each stream switches over only once the overlap
  is proven by its sequence key (`u` for depth and bookTicker, `a` for aggTrade, `E` for
  markPrice and klines; forceOrder is deduplicated by payload), so nothing is dropped or
  written twice.
- Server pings are counted per connection; a late server ping is logged.

After conversion, `to_parquet --verify` counts the breaks in the depth `pu` chain of a day and
compares them with the resyncs logged that day. Every break should have a logged resync.

## On-disk layout

By default all data goes under `<data root>/data/live`, where the data root is the current
working directory, or the directory named by the environment variable `RECORDER_ROOT`. Every
command also accepts `--root` to point at the `live` directory directly.

```
data/live/
  raw/{YYYY-MM-DD}/{SYMBOL}_{stream}.jsonl.zst      UTC date of the local receive time
  parquet/{stream}/{SYMBOL}/{YYYY-MM-DD}.parquet    zstd-compressed Parquet
  logs/recorder-{YYYY-MM-DD}.jsonl                  JSON-lines log
  heartbeat.json                                    status, rewritten every minute
```

Every flush (default every 5 seconds) appends one complete zstd frame, so a crash loses at most
the unflushed buffer and the file stays readable. Files rotate at 00:00 UTC.

Parquet types: timestamps `timestamp[ms, UTC]`, prices and quantities `decimal128(18, 8)`,
notional `decimal128(24, 8)`, ids `int64`, book levels `list<struct<price, qty>>`. No floats.

## Quick start

Requires Python 3.13 and [uv](https://docs.astral.sh/uv/).

```
uv sync
uv run pytest -q
```

A 30-minute test run (connects to Binance):

```
uv run python -m binance_futures_recorder --duration 1800 --echo
```

Recorder options:

| Option | Default | Meaning |
|---|---|---|
| `--symbols` | `BTCUSDT ETHUSDT` | symbols to record (choices: `BTCUSDT`, `ETHUSDT`) |
| `--root` | `<data root>/data/live` | output directory |
| `--flush-seconds` | `5.0` | how often buffered lines are written |
| `--rollover-hours` | `23.0` | connection age before a rollover (must be below 24) |
| `--min-free-gb` | `20.0` | log `disk_low` below this much free disk space |
| `--duration` | none | stop after N seconds (test runs) |
| `--echo` | off | also print log lines (interactive use only) |

Ctrl+C stops cleanly: buffers are flushed and a final heartbeat is written. If the environment
variable `RECORDER_HEALTHCHECKS_URL` is set, that URL is requested once a minute (for an
external uptime monitor such as Healthchecks.io).

For unattended runs on Windows, `scripts/run_recorder.ps1` restarts the recorder after any
exit and logs to `data/live/logs/run_recorder.log`:

```
powershell -ExecutionPolicy Bypass -File scripts\run_recorder.ps1
```

Health check of a running recorder, from its heartbeat (exit code 0 = healthy, 1 = stale
heartbeat, unsynced book, late server ping or low disk):

```
uv run python -m binance_futures_recorder.health
uv run python -m binance_futures_recorder.health --root <dir> --min-free-gb 8
```

Without `--min-free-gb` the threshold the recorder itself used is taken from the heartbeat.

Conversion to Parquet:

```
uv run python -m binance_futures_recorder.to_parquet                          # every complete UTC day
uv run python -m binance_futures_recorder.to_parquet --date 2026-10-31        # one day
uv run python -m binance_futures_recorder.to_parquet --date 2026-10-31 --delete-raw
uv run python -m binance_futures_recorder.to_parquet --verify 2026-10-31      # chain/gap report
uv run python -m binance_futures_recorder.to_parquet --inventory              # '<file>\t<rows>' per Parquet file
```

- A day is converted only when it is complete (before today, UTC) unless it is named with
  `--date`. Files already converted from a raw file of the same size are skipped; `--force`
  converts again.
- `--delete-raw` deletes a raw file only after its Parquet file was read back with the same
  row count as the raw file's line count. It is refused for today's date.
- `--verify` prints, per symbol, the depth row count, `pu` chain breaks, logged resyncs and
  aggTrade id gaps, then `OK` (exit 0) when every chain break has a logged resync, otherwise
  `CHECK` (exit 1). A day without depth data also gives `CHECK`.
- All three accept `--root`.

The test suite runs without network access. One test that connects to Binance for 10 seconds
is deselected by default; run it with `uv run pytest -q -m network`.

## What it does not do

- It does not trade, place orders, or use API keys.
- It covers Binance USD-M futures only, and the CLI only the two symbols listed above.
- It does not reconstruct the order book from the stored files for you; the raw diff events
  and snapshots are stored so that you can.
- It does not backfill: a period during which it was not running is missing. Historical data
  has to come from elsewhere (for example Binance's public archive).
- It has no built-in alerting beyond the log, the heartbeat file, the health command and the
  optional ping URL.

## Status

Version 0.1.0. Extracted from a private trading-research project, where it has run unattended
for weeks.

## Disclaimer

This project is not affiliated with, endorsed by, or connected to Binance. It records public
market data only. Nothing here is financial advice. Check Binance's terms of use for how you
may use the data you record.

## Licence

MIT, see `LICENSE`.
