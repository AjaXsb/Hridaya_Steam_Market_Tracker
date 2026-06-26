"""WebSocket live-push layer for the read API.

The REST endpoints serve cold-start snapshots; this module pushes the deltas.
The data lands in a SEPARATE process (cerebro's schedulers write the four data
tables), so this process can't know about a write directly — it learns via the
Postgres NOTIFY emitted by the data-table triggers (see
utility/marketDataNotify_utility). The chain:

    cerebro INSERT -> trigger -> pg_notify('market_data', {name, stream})
        -> this process' LISTEN connection
        -> re-read the latest row for (name, stream)
        -> push it to every WebSocket subscribed to that (name, stream)

The Postgres channel is a dumb broadcast (every write, every item). The
per-client filtering is the in-memory SubscriptionRegistry here: a NOTIFY for an
item nobody is watching is dropped without a read. That's what keeps one client's
single open card from receiving the whole firehose.

Trust model: same as REST — no auth, the dev frontend is trusted. No Origin
check on the WS handshake (browsers don't enforce CORS on WebSockets and we
chose not to gate it this pass).
"""

import asyncio
import json
from typing import Callable

import asyncpg
from fastapi import WebSocket

from utility.marketDataNotify_utility import MARKET_DATA_CHANNEL


class SubscriptionRegistry:
    """In-memory map of (market_hash_name, stream) -> set of WebSockets.

    The routing table the NOTIFY handler consults: which live sockets want a
    given item+stream. Guarded by a lock because subscribe/unsubscribe (from the
    WS recv loops) and reads (from the single NOTIFY handler) all touch it on the
    same event loop — the lock keeps a fan-out from racing a disconnect cleanup.
    """

    def __init__(self):
        self._subs: dict[tuple[str, str], set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, name: str, stream: str, ws: WebSocket) -> None:
        """Register a socket's interest in one (name, stream)."""
        async with self._lock:
            self._subs.setdefault((name, stream), set()).add(ws)

    async def unsubscribe(self, name: str, stream: str, ws: WebSocket) -> None:
        """Drop a socket's interest in one (name, stream), pruning the bucket if
        it empties (so an unwatched key holds no memory)."""
        async with self._lock:
            bucket = self._subs.get((name, stream))
            if bucket is None:
                return
            bucket.discard(ws)
            if not bucket:
                del self._subs[(name, stream)]

    async def drop_socket(self, ws: WebSocket) -> None:
        """Remove a socket from every key it subscribed to. Called once on
        disconnect so a dead socket leaves no dangling subscriptions."""
        async with self._lock:
            for key in list(self._subs.keys()):
                bucket = self._subs[key]
                bucket.discard(ws)
                if not bucket:
                    del self._subs[key]

    async def sockets_for(self, name: str, stream: str) -> list[WebSocket]:
        """Snapshot of sockets currently subscribed to (name, stream). Returns a
        list copy so the caller can fan out without holding the lock across the
        (awaiting) sends."""
        async with self._lock:
            bucket = self._subs.get((name, stream))
            return list(bucket) if bucket else []


def build_update_message(name: str, stream: str, data) -> str:
    """Serialize one server->client update frame.

    `data` is a Pydantic response model (OverviewResponse | BookSnapshot |
    ActivityResponse | HistoryResponse) — the SAME shape the matching REST GET
    returns, so the frontend parses a WS update exactly like a fetched payload.
    model_dump(mode='json') applies the models' serializers (ISO timestamps,
    etc.) so the frame matches REST byte-for-byte.
    """
    payload = data.model_dump(mode="json") if data is not None else None
    return json.dumps({"type": "update", "stream": stream, "name": name, "data": payload})


async def push_latest_to_subscribers(
    pool: asyncpg.Pool,
    registry: SubscriptionRegistry,
    read_ws_delta: Callable,
    name: str,
    stream: str,
) -> None:
    """Re-read the latest (name, stream) row and push it to its subscribers.

    Short-circuits before touching the DB when nobody is watching — the NOTIFY
    fires for every write, so the no-subscriber drop is the common path. Dead
    sockets that error on send are pruned from the registry.
    """
    sockets = await registry.sockets_for(name, stream)
    if not sockets:
        return  # nobody watching this item+stream — skip the read entirely

    async with pool.acquire() as conn:
        data = await read_ws_delta(conn, name, stream)
    if data is None:
        return  # no row to push yet (e.g. fresh history with nothing stored)

    message = build_update_message(name, stream, data)
    for ws in sockets:
        try:
            await ws.send_text(message)
        except Exception:
            # Send failed -> socket is gone; prune it. The disconnect handler
            # usually beats us to this, but a mid-fan-out drop lands here.
            await registry.drop_socket(ws)


async def listen_for_market_data(
    dsn: str,
    pool: asyncpg.Pool,
    registry: SubscriptionRegistry,
    read_ws_delta: Callable,
) -> asyncpg.Connection:
    """Open the dedicated LISTEN connection and wire the NOTIFY handler.

    A dedicated connection because asyncpg LISTEN can't share a pool connection
    (same constraint cerebro's listener hits). Returns the connection so the
    caller (lifespan) can close it on shutdown.

    The asyncpg callback is synchronous and must not block, so it schedules the
    actual read+push as a task on the loop rather than awaiting inline. Unlike
    cerebro we do NOT coalesce: each insert is a distinct data point the client
    wants, so every NOTIFY pushes (no debounce window).
    """
    conn = await asyncpg.connect(dsn)

    def on_notify(_conn, _pid, _channel, payload: str) -> None:
        try:
            msg = json.loads(payload)
            name, stream = msg["name"], msg["stream"]
        except (json.JSONDecodeError, KeyError):
            return  # malformed payload — ignore rather than crash the listener
        asyncio.create_task(
            push_latest_to_subscribers(pool, registry, read_ws_delta, name, stream)
        )

    await conn.add_listener(MARKET_DATA_CHANNEL, on_notify)
    print(f"  ✓ WS listening on '{MARKET_DATA_CHANNEL}' — new rows push to subscribers")
    return conn
