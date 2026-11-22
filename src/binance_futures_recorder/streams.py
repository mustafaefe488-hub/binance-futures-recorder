"""Stream catalogue, routing and per-stream keys for Binance USD-M market streams.

Binance split the WebSocket base URL (verified 2026-10-31 in "Important WebSocket Change
Notice — Base URL Split & Migration"): high-frequency public data (diff depth, bookTicker) is
served only on ``/public``; regular market data (aggTrade, markPrice, kline, forceOrder) only
on ``/market``. Un-routed legacy URLs were decommissioned after 2026-04-23 and deliver public
data only, so the recorder keeps one combined connection per route.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

WS_BASE = "wss://fstream.binance.com"
REST_BASE = "https://fapi.binance.com"
SNAPSHOT_LIMIT = 1000  # GET /fapi/v1/depth weight 20


class Route(StrEnum):
    PUBLIC = "public"
    MARKET = "market"


class StreamKind(StrEnum):
    DEPTH = "depth"
    AGG_TRADE = "aggTrade"
    BOOK_TICKER = "bookTicker"
    MARK_PRICE = "markPrice"
    FORCE_ORDER = "forceOrder"
    KLINE_1M = "kline_1m"

    @property
    def suffix(self) -> str:
        return {
            StreamKind.DEPTH: "depth@100ms",
            StreamKind.MARK_PRICE: "markPrice@1s",
        }.get(self, self.value)

    @property
    def route(self) -> Route:
        if self in (StreamKind.DEPTH, StreamKind.BOOK_TICKER):
            return Route.PUBLIC
        return Route.MARKET


# Records the recorder itself produces (not exchange streams).
DEPTH_SNAPSHOT = "depthSnapshot"
BOOK_CHECK = "bookCheck"
EVENTS = "events"
GLOBAL_SYMBOL = "ALL"


def stream_name(symbol: str, kind: StreamKind) -> str:
    return f"{symbol.lower()}@{kind.suffix}"


def route_url(route: Route, symbols: list[str]) -> str:
    names = [stream_name(s, k) for s in symbols for k in StreamKind if k.route is route]
    return f"{WS_BASE}/{route.value}/stream?streams={'/'.join(names)}"


_BY_SUFFIX = {k.suffix: k for k in StreamKind}


def parse_stream(name: str) -> tuple[str, StreamKind]:
    """``btcusdt@depth@100ms`` -> ``("BTCUSDT", StreamKind.DEPTH)``."""
    symbol, _, suffix = name.partition("@")
    try:
        return symbol.upper(), _BY_SUFFIX[suffix]
    except KeyError as exc:
        raise ValueError(f"unknown stream {name!r}") from exc


def dedupe_key(kind: StreamKind, data: dict[str, Any]) -> int | None:
    """Strictly increasing per-stream key used to drop duplicates across overlapping
    connections; None means the stream has no such key (forceOrder: payload-based)."""
    match kind:
        case StreamKind.DEPTH | StreamKind.BOOK_TICKER:
            return int(data["u"])
        case StreamKind.AGG_TRADE:
            return int(data["a"])
        case StreamKind.MARK_PRICE | StreamKind.KLINE_1M:
            return int(data["E"])
        case StreamKind.FORCE_ORDER:
            return None
