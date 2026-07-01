"""FastAPI read-path application for the CS2 market data store.

Five read-only endpoints serving the frontend from the existing
Postgres/Timescale instance. The connection pool is opened once on startup
(lifespan) and a connection is borrowed per request. This process never
writes to the database and is independent of ingestion/the schedulers.

Run with:  uvicorn api.main:app --reload --port 8000
Docs at:   http://localhost:8000/docs
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from api.databasePool import holder, open_read_pool
from api.marketDataStream import (
    SubscriptionRegistry,
    build_update_message,
    listen_for_market_data,
)
from api.responseModels import (
    ActivityResponse,
    BookSnapshot,
    HistoryResponse,
    MAX_POLL_INTERVAL_SEC,
    MIN_POLL_INTERVAL_SEC,
    MetaResponse,
    OverviewResponse,
    PRICEHISTORY_POLL_SEC,
    RateLimitState,
    TrackedItem,
    TrackedItemCreate,
    TrackedItemPatch,
    TrackingAck,
    TradeEvent,
    VALID_STREAMS,
)
from utility.configTableSync_utility import (
    regenerate_config_from_table,
    resolve_item_nameid,
)
from utility.feasibility_utility import compute_feasibility
from utility.loadTrackedItems_utility import STEAM_CURRENCY_ID_TO_ISO

# Streams that cannot poll without a resolved item_nameid (resolved server-side
# on write, never supplied by the client).
NAMEID_REQUIRED_STREAMS = ("histogram", "activity")

# Load CS2_PG_DSN (and friends) from .env, same as the ingestion entrypoints.
load_dotenv()

# Frontend origin(s) allowed by CORS. Browsers block cross-origin requests
# without these headers, so the dev frontend at :3000 must be listed.
# Production origins come from CORS_ALLOWED_ORIGINS (comma-separated) so the
# deployed frontend can be whitelisted without a code change; the localhost
# pair stays as the dev default.
def parse_allowed_origins_from_env() -> list[str]:
    """Build the CORS allowlist: env-supplied origins plus localhost dev pair."""
    defaults = ["http://localhost:3000", "http://127.0.0.1:3000"]
    raw = os.getenv("CORS_ALLOWED_ORIGINS", "")
    extra = [o.strip() for o in raw.split(",") if o.strip()]
    # De-dupe while preserving order.
    return list(dict.fromkeys(extra + defaults))


ALLOWED_ORIGINS = parse_allowed_origins_from_env()

# Cold-start population sizes, fixed per stream (no client window params this
# pass — see the GET docstrings). Overview = live chart tip; activity = enough
# tape to read.
OVERVIEW_LIMIT = 200
ACTIVITY_TAIL = 50

# Maps a tracked stream to the GET shape that serves it, so POST/PATCH can call
# the SAME shared read the standalone GET uses (one read per stream, no dupes).
STREAM_TO_READER = {}  # populated below, once the reader fns are defined

# range= param -> SQL interval. "all" means no lower bound.
HISTORY_RANGES = {
    "week": "7 days",
    "month": "30 days",
    "year": "365 days",
    "all": None,
}


# Process-wide WebSocket subscription registry: (name, stream) -> sockets. The
# NOTIFY listener consults it to route a fresh row to only the clients watching
# that item+stream. read_ws_delta_for_stream is defined further down (after the
# readers); the listener is spawned in lifespan, by which point it exists.
ws_registry = SubscriptionRegistry()


class IngestionHandle:
    """Holds the in-process cerebro orchestrator (if any), so the status
    endpoints can report whether ingestion is live without reaching into
    lifespan locals. orchestrator is None when RUN_INGESTION is off."""

    orchestrator = None  # set in lifespan when ingestion is enabled


ingestion = IngestionHandle()


def ingestion_enabled() -> bool:
    """Whether to run cerebro in-process. Combined deploy (one Render web
    service hosts API + ingestion) sets RUN_INGESTION=1; unset/0 runs API only
    (e.g. local dev with cerebro started separately)."""
    return os.getenv("RUN_INGESTION", "0").lower() in ("1", "true", "yes")


def seed_tracked_set_from_config() -> bool:
    """Whether cerebro replays config.yaml -> tracked_items on boot. Off by
    default for the scale-to-zero showcase so each wake polls only what the
    frontend requests; set SEED_FROM_CONFIG=1 to restore config-seeded boots."""
    return os.getenv("SEED_FROM_CONFIG", "0").lower() in ("1", "true", "yes")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the pool at startup, spawn the market-data NOTIFY listener, optionally
    run the cerebro ingestion orchestrator in-process, and tear all down at
    shutdown.

    The listener opens its OWN dedicated connection (asyncpg LISTEN can't share a
    pool connection), held here so it closes cleanly on shutdown. When
    RUN_INGESTION is set, the cerebro Orchestrator runs as a background task so a
    single Render web service hosts both the API and ingestion; it owns no signal
    handlers (the web server does) and is stopped via its shutdown_event.
    """
    holder.pool = await open_read_pool()
    listen_conn = await listen_for_market_data(
        os.getenv("CS2_PG_DSN"),
        holder.pool,
        ws_registry,
        read_ws_delta_for_stream,
    )

    orchestrator = None
    ingestion_task = None
    if ingestion_enabled():
        # Imported lazily so API-only runs never pull in the ingestion stack.
        from cerebro import Orchestrator

        orchestrator = Orchestrator(config_path="config.yaml")
        ingestion.orchestrator = orchestrator
        ingestion_task = asyncio.create_task(
            orchestrator.run(
                install_signal_handlers=False,
                seed_from_config=seed_tracked_set_from_config(),
            ),
            name="cerebro-ingestion",
        )

    try:
        yield
    finally:
        ingestion.orchestrator = None
        # Stop ingestion first (clean path via its shutdown_event), then drop
        # the listener and pool it shares with the API.
        if orchestrator is not None and ingestion_task is not None:
            orchestrator.shutdown_event.set()
            try:
                await asyncio.wait_for(ingestion_task, timeout=30)
            except asyncio.TimeoutError:
                ingestion_task.cancel()
                try:
                    await ingestion_task
                except asyncio.CancelledError:
                    pass
        await listen_conn.close()
        if holder.pool is not None:
            await holder.pool.close()


app = FastAPI(
    title="CS2 Market Read API",
    description="Read-only access to tracked CS2 market data.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    allow_headers=["*"],
)


@app.get("/health")
async def report_liveness():
    """Cheap liveness probe. On a scale-to-zero host the FIRST hit is what wakes
    the service; it returns as soon as the HTTP server is up, before ingestion
    has finished booting. The frontend calls this to trigger the wake, then polls
    /ingestion-status for readiness."""
    return {"status": "ok"}


@app.get("/ingestion-status")
async def report_ingestion_status():
    """Readiness of in-process ingestion, for the scale-to-zero wake flow.

    state:
      - "disabled": RUN_INGESTION is off — API runs without ingestion.
      - "booting":  orchestrator spawned, schedulers/listener not live yet.
      - "ready":    pollers + change-listener live; frontend POSTs to
                    /tracked-items now take effect and the frontend can render.
    """
    orch = ingestion.orchestrator
    if orch is None:
        return {"state": "disabled", "ready": False}
    ready = orch.ready_event.is_set()
    return {"state": "ready" if ready else "booting", "ready": ready}


def parse_price_to_float(value) -> Optional[float]:
    """Coerce a stored activity price (string or number) to float, or None."""
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Shared reads — the single read function per live stream.
#
# Each returns the response model for cold-start card population, ALWAYS shaped
# identically whether the DB is empty, partial, or full: an empty payload (no
# points/events, null currency) when nothing is stored yet, never an error.
# These are the SAME callables the GET endpoints, POST, and PATCH all invoke —
# one read per stream, no duplicated SQL (same discipline as compute_feasibility).
# The 200-empty vs 404 decision is NOT made here; it lives in the GET wrapper,
# which only 404s when the item also isn't in the tracked set.
# ---------------------------------------------------------------------------


async def read_recent_overview(conn, name: str) -> OverviewResponse:
    """Recent intraday priceoverview series (newest first), sized for the live
    chart tip. Empty payload when the item has no overview rows yet."""
    rows = await conn.fetch(
        """
        SELECT timestamp, currency, lowest_price, median_price, volume
        FROM price_overview
        WHERE market_hash_name = $1
        ORDER BY timestamp DESC
        LIMIT $2
        """,
        name,
        OVERVIEW_LIMIT,
    )
    if not rows:
        return OverviewResponse()
    points = [dict(r) for r in rows]
    return OverviewResponse(currency=points[0]["currency"], points=points)


async def read_latest_orderbook(conn, name: str) -> BookSnapshot:
    """Single most-recent order-book histogram snapshot. Empty payload (only the
    name filled, everything else null) when no snapshot exists yet.

    The JSONB order tables/graphs come back as native structured arrays via the
    connection's JSONB codec — passed through unchanged.
    """
    row = await conn.fetchrow(
        """
        SELECT market_hash_name, timestamp, currency,
               buy_order_table, sell_order_table,
               buy_order_graph, sell_order_graph,
               buy_order_count, sell_order_count,
               highest_buy_order, lowest_sell_order
        FROM orders_histogram
        WHERE market_hash_name = $1
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        name,
    )
    if row is None:
        return BookSnapshot(market_hash_name=name)
    return BookSnapshot(**dict(row))


async def read_recent_activity(conn, name: str) -> ActivityResponse:
    """Recent tail of trades from the latest activity snapshot (last
    ACTIVITY_TAIL events), enough to read the tape. Empty payload when no
    snapshot exists yet."""
    row = await conn.fetchrow(
        """
        SELECT currency, parsed_activities
        FROM orders_activity
        WHERE market_hash_name = $1
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        name,
    )
    if row is None:
        return ActivityResponse()
    parsed = (row["parsed_activities"] or [])[-ACTIVITY_TAIL:]
    events = [
        TradeEvent(
            timestamp=e.get("timestamp"),
            currency=e.get("currency") or row["currency"],
            action=e.get("action"),
            price=parse_price_to_float(e.get("price")),
        )
        for e in parsed
    ]
    return ActivityResponse(currency=row["currency"], events=events)


async def read_recent_history(conn, name: str):
    """Full stored price-history series for an item (oldest first), the archival
    parallel to the live readers. Returns None when no history rows exist yet —
    HistoryResponse requires a currency, so an empty payload can't be shaped; a
    fresh add just seeds nothing and the first hourly fetch fills it in.

    Unlike the GET /history endpoint this is unbounded (no range param): it seeds
    the frontend cache with everything on hand at write time, same spirit as the
    other readers returning their full cold-start payload.
    """
    rows = await conn.fetch(
        """
        SELECT time AS timestamp, currency, price, volume
        FROM price_history
        WHERE market_hash_name = $1
        ORDER BY time ASC
        """,
        name,
    )
    if not rows:
        return None
    points = [dict(r) for r in rows]
    return HistoryResponse(currency=points[0]["currency"], points=points)


# Wire each stream to its single reader. POST/PATCH look the item's stream up
# here so they return current data via the exact same callable the GET uses.
STREAM_TO_READER.update({
    "priceoverview": read_recent_overview,
    "histogram": read_latest_orderbook,
    "activity": read_recent_activity,
    "pricehistory": read_recent_history,
})


# ---------------------------------------------------------------------------
# Latest-1 readers — the per-tick delta the WebSocket pushes.
#
# REST GETs serve the bulk cold-start series (200 overview pts, whole history).
# A WS tick only needs the ONE freshly-written row: the frontend already holds
# the series and appends. The append streams (overview/pricehistory) need a
# single-point variant; histogram/activity are inherently latest-1 already, so
# their bulk readers double as the WS readers (see STREAM_TO_WS_READER).
# ---------------------------------------------------------------------------


async def read_single_latest_overview(conn, name: str) -> OverviewResponse:
    """Single newest priceoverview point — the WS delta the chart tip appends.

    Same shape as read_recent_overview (OverviewResponse) but one point, so the
    frontend parses a WS tick exactly like a REST point. Empty payload when no
    rows yet."""
    row = await conn.fetchrow(
        """
        SELECT timestamp, currency, lowest_price, median_price, volume
        FROM price_overview
        WHERE market_hash_name = $1
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        name,
    )
    if row is None:
        return OverviewResponse()
    point = dict(row)
    return OverviewResponse(currency=point["currency"], points=[point])


async def read_single_latest_history_point(conn, name: str):
    """Single newest price_history point — the WS delta appended to the archival
    series. Returns None when no rows yet (HistoryResponse requires a currency,
    same constraint as read_recent_history)."""
    row = await conn.fetchrow(
        """
        SELECT time AS timestamp, currency, price, volume
        FROM price_history
        WHERE market_hash_name = $1
        ORDER BY time DESC
        LIMIT 1
        """,
        name,
    )
    if row is None:
        return None
    point = dict(row)
    return HistoryResponse(currency=point["currency"], points=[point])


# Per-stream WS reader: the latest-1 delta a NOTIFY re-reads and pushes. The
# append streams use the single-point variants above; histogram/activity reuse
# their bulk readers (already latest-1: one order book, one tape tail).
STREAM_TO_WS_READER = {
    "priceoverview": read_single_latest_overview,
    "histogram": read_latest_orderbook,
    "activity": read_recent_activity,
    "pricehistory": read_single_latest_history_point,
}


async def read_ws_delta_for_stream(conn, name: str, stream: str):
    """Dispatch to the latest-1 reader for `stream` — the delta the WS pushes on
    a NOTIFY and on a fresh subscribe."""
    reader = STREAM_TO_WS_READER.get(stream)
    if reader is None:
        return None
    return await reader(conn, name)


async def is_item_tracked(conn, name: str) -> bool:
    """True if the name is in the enabled tracked set (any stream).

    The load-bearing seam: a live GET that finds no data uses this to tell
    'tracked, still collecting' (-> 200 empty) from 'not tracked at all'
    (-> 404). Data tables alone can't make that call — emptiness looks the same
    either way until you consult the tracked set.
    """
    return await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM tracked_items WHERE market_hash_name = $1 AND enabled = TRUE)",
        name,
    )


async def read_current_for_stream(conn, name: str, stream: str):
    """Dispatch to the one reader for `stream` — the shared read POST/PATCH embed
    in their response so the frontend seeds its cache without a round-trip."""
    reader = STREAM_TO_READER.get(stream)
    if reader is None:
        return None  # unknown stream: no seed
    return await reader(conn, name)


@app.get("/items", response_model=list[TrackedItem])
async def list_tracked_items():
    """Return the tracked set from tracked_items (the source of truth).

    stream and poll_interval_sec come straight from the backend, so the
    frontend no longer guesses cadence/stream. Items appear here whether or not
    they have data yet — this reads the tracked set, not the data tables.
    """
    async with holder.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT market_hash_name, appid, item_nameid,
                   stream, currency, poll_interval_sec
            FROM tracked_items
            WHERE enabled = TRUE
            ORDER BY market_hash_name, stream
            """
        )
    return [
        TrackedItem(
            market_hash_name=r["market_hash_name"],
            appid=r["appid"],
            item_nameid=r["item_nameid"],
            stream=r["stream"],
            currency=STEAM_CURRENCY_ID_TO_ISO.get(r["currency"], str(r["currency"])),
            poll_interval_sec=r["poll_interval_sec"],
        )
        for r in rows
    ]


@app.get("/meta", response_model=MetaResponse)
async def get_operational_meta():
    """Return operational state for the header.

    - tracked_count: enabled tracked items (real).
    - rate_limit: configured budget from config.yaml. "used" is derived live from
      the DB: the limiter's in-memory state lives in the scheduler process and
      isn't reachable cross-process, but every Steam call that consumes a token
      writes exactly one row (server-side NOW()) into one of the three live
      snapshot tables, so counting rows in the last window_seconds reconstructs
      the count the limiter holds. price_history is excluded by construction:
      one call writes many rows stamped with each point's historical date, not
      NOW(), so they never fall inside the window.
    - last_ingest: most recent write across the three live snapshot tables.
    """
    with open("config.yaml") as f:
        limits = yaml.safe_load(f)["LIMITS"]
    window_seconds = limits["WINDOW_SECONDS"]

    async with holder.pool.acquire() as conn:
        tracked_count = await conn.fetchval(
            "SELECT count(*) FROM tracked_items WHERE enabled = TRUE"
        )
        last_ingest = await conn.fetchval(
            """
            SELECT max(ts) FROM (
                SELECT max(timestamp) AS ts FROM price_overview
                UNION ALL
                SELECT max(timestamp) AS ts FROM orders_histogram
                UNION ALL
                SELECT max(timestamp) AS ts FROM orders_activity
            ) t
            """
        )
        used = await conn.fetchval(
            """
            SELECT
                (SELECT count(*) FROM price_overview   WHERE timestamp >= $1)
              + (SELECT count(*) FROM orders_histogram WHERE timestamp >= $1)
              + (SELECT count(*) FROM orders_activity  WHERE timestamp >= $1)
            """,
            datetime.now(timezone.utc) - timedelta(seconds=window_seconds),
        )

    rate_limit = RateLimitState(
        used=used,
        limit=limits["REQUESTS"],
        window_seconds=window_seconds,
        used_is_live=True,
        note="'used' derived live from row inserts across the three live "
        "snapshot tables in the last window_seconds (one request = one row).",
    )

    return MetaResponse(
        tracked_count=tracked_count,
        rate_limit=rate_limit,
        last_ingest=last_ingest,
    )


@app.get("/overview/{name}", response_model=OverviewResponse)
async def get_recent_overview(name: str):
    """Recent intraday priceoverview series for the live chart tip.

    Tracked-but-empty -> 200 empty payload; untracked -> 404. Fixed size
    (OVERVIEW_LIMIT), no window params — the client zooms within the fetched
    bucket.
    """
    async with holder.pool.acquire() as conn:
        result = await read_recent_overview(conn, name)
        if not result.points and not await is_item_tracked(conn, name):
            raise HTTPException(status_code=404, detail=f"'{name}' is not tracked")
    return result


@app.get("/history/{name}", response_model=HistoryResponse)
async def get_price_history(
    name: str,
    range: str = Query("month"),
):
    """Return price history for an item, bounded by the requested range.

    Tracked-but-empty -> 200 empty payload (currency=None, points=[]); untracked
    -> 404. Same 200-empty-vs-404 discipline as the live read endpoints, so a
    freshly added item still collecting reads as 200 empty rather than 404.
    """
    if range not in HISTORY_RANGES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid range '{range}'. Use one of: {', '.join(HISTORY_RANGES)}",
        )
    interval = HISTORY_RANGES[range]

    async with holder.pool.acquire() as conn:
        if interval is None:
            rows = await conn.fetch(
                """
                SELECT time AS timestamp, currency, price, volume
                FROM price_history
                WHERE market_hash_name = $1
                ORDER BY time ASC
                """,
                name,
            )
        else:
            # `interval` is from the trusted HISTORY_RANGES dict, never user
            # input, so embedding it as a literal is safe. It can't be bound as
            # a parameter because asyncpg encodes interval params as timedeltas.
            rows = await conn.fetch(
                f"""
                SELECT time AS timestamp, currency, price, volume
                FROM price_history
                WHERE market_hash_name = $1
                  AND time >= NOW() - INTERVAL '{interval}'
                ORDER BY time ASC
                """,
                name,
            )
        if not rows:
            # Empty: distinguish 'tracked, still collecting' (200 empty) from
            # 'not tracked at all' (404) — same seam as the live GETs.
            if not await is_item_tracked(conn, name):
                raise HTTPException(status_code=404, detail=f"'{name}' is not tracked")
            return HistoryResponse()

    points = [dict(r) for r in rows]
    return HistoryResponse(currency=points[0]["currency"], points=points)


@app.get("/orderbook/{name}", response_model=BookSnapshot)
async def get_latest_orderbook(name: str):
    """Single most-recent order-book snapshot for an item.

    Tracked-but-empty -> 200 empty payload (name filled, rest null);
    untracked -> 404.
    """
    async with holder.pool.acquire() as conn:
        result = await read_latest_orderbook(conn, name)
        if result.timestamp is None and not await is_item_tracked(conn, name):
            raise HTTPException(status_code=404, detail=f"'{name}' is not tracked")
    return result


@app.get("/activity/{name}", response_model=ActivityResponse)
async def get_latest_activity(name: str):
    """Recent tail of trades for an item, enough to read the tape.

    Tracked-but-empty -> 200 empty payload; untracked -> 404.
    """
    async with holder.pool.acquire() as conn:
        result = await read_recent_activity(conn, name)
        if not result.events and not await is_item_tracked(conn, name):
            raise HTTPException(status_code=404, detail=f"'{name}' is not tracked")
    return result


# ---------------------------------------------------------------------------
# Write path — the guarded front door for the tracked set.
#
# Each endpoint only VALIDATES + WRITES the table. The existing chain (trigger
# -> NOTIFY -> scheduler listener -> reconcile, plus config writeback) starts/
# stops/updates pollers automatically; nothing here talks to the scheduler.
#
# Feasibility is checked HERE, before the write, so the table never holds an
# item the scheduler would reject — a synchronous honest answer to the user.
# The listener's gate stays the final guard (e.g. for direct SQL). Both call
# the SAME compute_feasibility (utility/feasibility_utility) — one rule, no
# drift.
# ---------------------------------------------------------------------------


def reject_and_log(status_code: int, detail: str):
    """Log the rejection to console, then raise it as an HTTP error.

    Console logging is a stopgap until the websocket layer streams write events
    to the frontend — for now stdout is the sanity view, mirroring the
    inbound-value prints cerebro emits on its side.
    """
    print(f"  ✗ rejected ({status_code}): {detail}")
    raise HTTPException(status_code, detail)


def read_rate_budget() -> tuple[int, int]:
    """(REQUESTS, WINDOW_SECONDS) from config.yaml — the same budget the
    scheduler validates against."""
    with open("config.yaml") as f:
        limits = yaml.safe_load(f)["LIMITS"]
    return limits["REQUESTS"], limits["WINDOW_SECONDS"]


async def fetch_enabled_intervals(conn, exclude_id: Optional[int] = None) -> list[int]:
    """Poll intervals of the currently enabled LIVE set, optionally excluding one
    row (so a PATCH measures the set WITHOUT the row it's about to change).

    pricehistory rows are excluded: clockwork runs them on a fixed hourly tick
    regardless of poll_interval_sec, so they add no sustained load and must not
    count toward the budget (mirrors cerebro's feasibility exclusion)."""
    if exclude_id is None:
        rows = await conn.fetch(
            "SELECT poll_interval_sec FROM tracked_items "
            "WHERE enabled = TRUE AND stream <> 'pricehistory'"
        )
    else:
        rows = await conn.fetch(
            "SELECT poll_interval_sec FROM tracked_items "
            "WHERE enabled = TRUE AND stream <> 'pricehistory' AND id <> $1",
            exclude_id,
        )
    return [r["poll_interval_sec"] for r in rows]


async def resolve_target_row(conn, market_hash_name: str, stream: Optional[str]):
    """Find the single tracked_items row a write targets by its real unique key.

    (market_hash_name, stream) is the unique key — an item can be tracked on
    several streams, so name alone can be ambiguous. The internal autoincrement
    id is never accepted from callers; it stays a DB detail. The returned row
    still carries `id` for internal SQL, but nothing exposes it.

    - stream given: target that exact pair; 404 if it doesn't exist.
    - stream omitted: resolve by name; 404 if no row, 409 if more than one
      (caller must specify stream to disambiguate).
    """
    if stream is not None:
        row = await conn.fetchrow(
            "SELECT * FROM tracked_items WHERE market_hash_name = $1 AND stream = $2",
            market_hash_name, stream,
        )
        if row is None:
            reject_and_log(404, f"No tracked item '{market_hash_name}' on stream '{stream}'")
        return row

    rows = await conn.fetch(
        "SELECT * FROM tracked_items WHERE market_hash_name = $1 ORDER BY stream",
        market_hash_name,
    )
    if not rows:
        reject_and_log(404, f"No tracked item '{market_hash_name}'")
    if len(rows) > 1:
        streams = ", ".join(r["stream"] for r in rows)
        reject_and_log(
            409,
            f"'{market_hash_name}' is tracked on multiple streams ({streams}); "
            f"specify stream to target one.",
        )
    return rows[0]


async def mirror_config_after_write() -> None:
    """table -> config writeback (Mechanism 4), best-effort.

    The table is the master; config is the mirror that lets frontend/API-added
    items survive a scheduler reboot. Failure here (e.g. a read-only config
    file) must NOT fail the API call, so it's swallowed with a warning. The
    scheduler's watcher will observe this file change cross-process, but the
    no-op-suppressing upsert means its re-sync is a no-op — no write loop.
    """
    dsn = os.getenv("CS2_PG_DSN")
    try:
        await regenerate_config_from_table(dsn, "config.yaml")
    except Exception as e:
        print(f"  ⚠ config writeback failed (table still authoritative): {e}")


@app.post("/tracked-items", status_code=202, response_model=TrackingAck)
async def add_tracked_item(item: TrackedItemCreate):
    """Add ONE item to the tracked set (no batches).

    Validate -> resolve nameid (histogram/activity) -> feasibility pre-check
    (POST always adds load) -> write. A 202 means "tracked, collecting" — the
    first poll runs seconds later via the reconcile chain; it does NOT mean data
    exists yet.
    """
    # --- validate (untrusted body) ---
    if item.stream not in VALID_STREAMS:
        reject_and_log(400, f"Invalid stream '{item.stream}'. Use one of: {', '.join(VALID_STREAMS)}")
    if not item.market_hash_name.strip():
        reject_and_log(400, "market_hash_name must not be empty")
    if item.appid <= 0:
        reject_and_log(400, f"Invalid appid {item.appid} (must be positive; 730 = CS2)")
    if item.currency <= 0:
        reject_and_log(400, f"Invalid currency id {item.currency}")

    # Resolve the cadence: pricehistory has none (fixed hourly tick), so any
    # client value is ignored and the canonical hourly value is stamped. Live
    # streams must supply an in-bounds interval.
    if item.stream == "pricehistory":
        poll_interval = PRICEHISTORY_POLL_SEC
    else:
        if item.poll_interval_sec is None:
            reject_and_log(400, f"poll_interval_sec is required for the '{item.stream}' stream")
        if not (MIN_POLL_INTERVAL_SEC <= item.poll_interval_sec <= MAX_POLL_INTERVAL_SEC):
            reject_and_log(
                400,
                f"poll_interval_sec {item.poll_interval_sec} out of bounds "
                f"[{MIN_POLL_INTERVAL_SEC}, {MAX_POLL_INTERVAL_SEC}]",
            )
        poll_interval = item.poll_interval_sec

    print(f"\n📥 POST /tracked-items: '{item.market_hash_name}' | {item.stream} | "
          f"every {poll_interval}s | cur={item.currency} country={item.country} appid={item.appid}")

    # --- resolve item_nameid for streams that need it ---
    item_nameid = None
    if item.stream in NAMEID_REQUIRED_STREAMS:
        item_nameid = resolve_item_nameid(item.market_hash_name)
        if item_nameid is None:
            reject_and_log(
                400,
                f"Couldn't find '{item.market_hash_name}' on Steam — no item id "
                f"resolvable, which the '{item.stream}' stream requires.",
            )
        else:
            print(f"  ◆ resolved item_nameid={item_nameid}")

    async with holder.pool.acquire() as conn:
        # Duplicate: already tracked + enabled -> 409 (re-adding an enabled item).
        existing = await conn.fetchrow(
            "SELECT id, enabled FROM tracked_items WHERE market_hash_name = $1 AND stream = $2",
            item.market_hash_name, item.stream,
        )
        if existing and existing["enabled"]:
            reject_and_log(409, f"'{item.market_hash_name}' ({item.stream}) is already tracked")

        # --- feasibility pre-check BEFORE writing (POST adds load) ---
        # A pricehistory add contributes no sustained load (clockwork runs it on
        # a fixed hourly tick), so it isn't added to the budgeted intervals — it
        # can't fail this gate, but we still run it for the capacity log line.
        rate_limit, window = read_rate_budget()
        intervals = await fetch_enabled_intervals(conn)  # disabled re-adds aren't in here yet
        added_load = [] if item.stream == "pricehistory" else [poll_interval]
        ok, total, util = compute_feasibility(rate_limit, window, intervals + added_load)
        if not ok:
            reject_and_log(
                409,
                f"Would exceed rate limit: {total} req/{window}s vs budget {rate_limit}. "
                f"Increase poll_interval_sec or remove an item.",
            )

        # --- write (insert, or re-enable a previously disabled row) ---
        row = await conn.fetchrow(
            """
            INSERT INTO tracked_items
                (market_hash_name, appid, item_nameid, stream,
                 currency, country, language, poll_interval_sec, enabled)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
            ON CONFLICT (market_hash_name, stream) DO UPDATE SET
                appid = EXCLUDED.appid,
                item_nameid = EXCLUDED.item_nameid,
                currency = EXCLUDED.currency,
                country = EXCLUDED.country,
                language = EXCLUDED.language,
                poll_interval_sec = EXCLUDED.poll_interval_sec,
                enabled = TRUE
            RETURNING id
            """,
            item.market_hash_name, item.appid, item_nameid, item.stream,
            item.currency, item.country, item.language, poll_interval,
        )

        # Seed current data via the SAME reader the GET uses (pre-existing data
        # available at write time — usually empty on a fresh add, no poll wait).
        data = await read_current_for_stream(conn, item.market_hash_name, item.stream)

    await mirror_config_after_write()
    print(f"  ✓ tracking id={row['id']} ({total} req/{window}s, {util:.1f}% capacity) — "
          f"reconcile chain will start the poller")
    return TrackingAck(
        status="tracking",
        market_hash_name=item.market_hash_name,
        stream=item.stream,
        note="collecting first data",
        data=data,
    )


@app.patch("/tracked-items", response_model=TrackingAck)
async def modify_tracked_item(patch: TrackedItemPatch):
    """Modify one item's poll_interval_sec, stream, or enabled.

    The row is targeted by its real unique key (market_hash_name + stream), never
    the internal id. `new_stream` moves the row to a different stream.

    Feasibility is re-checked ONLY when the change increases load (interval
    decrease, or enabling a disabled row). Load-decreasing changes (interval
    increase, disabling) can't fail feasibility, so the check is skipped.
    """
    print(f"\n📥 PATCH /tracked-items: '{patch.market_hash_name}' (stream={patch.stream}) "
          f"-> interval={patch.poll_interval_sec} new_stream={patch.new_stream} enabled={patch.enabled}")

    if patch.poll_interval_sec is None and patch.new_stream is None and patch.enabled is None:
        reject_and_log(400, "Nothing to update: provide poll_interval_sec, new_stream, or enabled")
    if patch.new_stream is not None and patch.new_stream not in VALID_STREAMS:
        reject_and_log(400, f"Invalid stream '{patch.new_stream}'. Use one of: {', '.join(VALID_STREAMS)}")

    async with holder.pool.acquire() as conn:
        cur = await resolve_target_row(conn, patch.market_hash_name, patch.stream)
        item_id = cur["id"]

        new_stream = patch.new_stream if patch.new_stream is not None else cur["stream"]
        new_enabled = patch.enabled if patch.enabled is not None else cur["enabled"]

        # Resolve the cadence against the EFFECTIVE target stream: pricehistory
        # has none (fixed hourly tick), so any supplied interval is ignored and
        # the canonical hourly value is stamped. Live streams bounds-check a
        # supplied interval; an omitted one keeps the current value.
        if new_stream == "pricehistory":
            new_interval = PRICEHISTORY_POLL_SEC
        elif patch.poll_interval_sec is not None:
            if not (MIN_POLL_INTERVAL_SEC <= patch.poll_interval_sec <= MAX_POLL_INTERVAL_SEC):
                reject_and_log(
                    400,
                    f"poll_interval_sec {patch.poll_interval_sec} out of bounds "
                    f"[{MIN_POLL_INTERVAL_SEC}, {MAX_POLL_INTERVAL_SEC}]",
                )
            new_interval = patch.poll_interval_sec
        else:
            new_interval = cur["poll_interval_sec"]

        # Resolve nameid if the (new) stream needs one and we don't have it.
        new_nameid = cur["item_nameid"]
        if new_stream in NAMEID_REQUIRED_STREAMS and new_nameid is None:
            new_nameid = resolve_item_nameid(cur["market_hash_name"])
            if new_nameid is None:
                reject_and_log(
                    400,
                    f"Couldn't resolve an item id for '{cur['market_hash_name']}', "
                    f"required by the '{new_stream}' stream.",
                )
            else:
                print(f"  ◆ resolved item_nameid={new_nameid}")

        # Load increases only if the row will be enabled AND its per-window
        # contribution goes up vs its current contribution (0 if currently
        # disabled). Otherwise it frees budget -> skip the check. pricehistory
        # contributes 0 either way (fixed hourly tick, off the budget), so a
        # move onto/off it is handled by these zero contributions.
        rate_limit, window = read_rate_budget()

        def sustained_contribution(stream: str, interval: int, enabled: bool) -> int:
            if not enabled or stream == "pricehistory":
                return 0
            return window // interval

        old_contrib = sustained_contribution(cur["stream"], cur["poll_interval_sec"], cur["enabled"])
        new_contrib = sustained_contribution(new_stream, new_interval, new_enabled)
        if new_contrib > old_contrib:
            intervals = await fetch_enabled_intervals(conn, exclude_id=item_id)
            ok, total, util = compute_feasibility(rate_limit, window, intervals + [new_interval])
            if not ok:
                reject_and_log(
                    409,
                    f"Would exceed rate limit: {total} req/{window}s vs budget {rate_limit}. "
                    f"Pick a larger poll_interval_sec.",
                )

        await conn.execute(
            """
            UPDATE tracked_items
            SET poll_interval_sec = $1, stream = $2, enabled = $3, item_nameid = $4
            WHERE id = $5
            """,
            new_interval, new_stream, new_enabled, new_nameid, item_id,
        )

        # Seed current data via the SAME reader the GET uses, keyed on the row's
        # (possibly new) stream — pre-existing data at write time, no poll wait.
        data = await read_current_for_stream(conn, cur["market_hash_name"], new_stream)

    await mirror_config_after_write()
    print(f"  ✓ updated id={item_id}: '{cur['market_hash_name']}' | {new_stream} | "
          f"every {new_interval}s | enabled={new_enabled} — reconcile chain applies it")
    return TrackingAck(
        status="updated",
        market_hash_name=cur["market_hash_name"],
        stream=new_stream,
        note="reconciling live" if new_enabled else "disabled",
        data=data,
    )


@app.delete("/tracked-items", response_model=TrackingAck)
async def remove_tracked_item(
    market_hash_name: str = Query(..., description="Item to stop tracking"),
    stream: Optional[str] = Query(
        None, description="Stream to target; required when the name is tracked on more than one"
    ),
):
    """Remove one item by disabling it (enabled=FALSE).

    Targeted by its real unique key (market_hash_name + stream), never the
    internal id. stream is optional only so an ambiguous name (tracked on several
    streams) gets a clear 409 asking for it.

    Disable, not hard-delete: it preserves the row (and its tracking history)
    and is the safer default. Only frees budget, so no feasibility check.
    """
    print(f"\n📥 DELETE /tracked-items: '{market_hash_name}' (stream={stream})")
    async with holder.pool.acquire() as conn:
        cur = await resolve_target_row(conn, market_hash_name, stream)
        await conn.execute("UPDATE tracked_items SET enabled = FALSE WHERE id = $1", cur["id"])

    await mirror_config_after_write()
    print(f"  ✓ disabled '{cur['market_hash_name']}' ({cur['stream']}) — "
          f"poller stops on reconcile")
    return TrackingAck(
        status="disabled",
        market_hash_name=cur["market_hash_name"],
        stream=cur["stream"],
        note="poller stops on reconcile",
    )


@app.put("/tracked-items")
async def replace_tracked_set(items: list[TrackedItemCreate]):
    """Replace the ENTIRE enabled tracked set with `items` in one call.

    Declarative, unlike POST (which adds one): after this returns, exactly the
    items in the body are enabled and every other row is disabled. Built for the
    scale-to-zero wake flow — the frontend declares the whole set it wants on
    each cold start without diffing the current set itself. An empty body
    disables everything ("track nothing").

    All-or-nothing: the whole desired set is validated, nameid-resolved, and
    feasibility-gated FIRST, then enable/disable is applied in one transaction.
    A bad item or an over-budget set rejects the entire call and changes nothing.
    """
    # --- validate + normalize each desired item; reject dup keys in the body ---
    normalized = []
    seen = set()
    for item in items:
        if item.stream not in VALID_STREAMS:
            reject_and_log(400, f"Invalid stream '{item.stream}'. Use one of: {', '.join(VALID_STREAMS)}")
        if not item.market_hash_name.strip():
            reject_and_log(400, "market_hash_name must not be empty")
        if item.appid <= 0:
            reject_and_log(400, f"Invalid appid {item.appid} (must be positive; 730 = CS2)")
        if item.currency <= 0:
            reject_and_log(400, f"Invalid currency id {item.currency}")

        key = (item.market_hash_name, item.stream)
        if key in seen:
            reject_and_log(400, f"Duplicate item in body: '{item.market_hash_name}' ({item.stream})")
        seen.add(key)

        # Cadence: pricehistory is a fixed hourly tick (client value ignored);
        # live streams require an in-bounds interval.
        if item.stream == "pricehistory":
            poll_interval = PRICEHISTORY_POLL_SEC
        else:
            if item.poll_interval_sec is None:
                reject_and_log(400, f"poll_interval_sec is required for the '{item.stream}' stream ('{item.market_hash_name}')")
            if not (MIN_POLL_INTERVAL_SEC <= item.poll_interval_sec <= MAX_POLL_INTERVAL_SEC):
                reject_and_log(400, f"poll_interval_sec {item.poll_interval_sec} out of bounds [{MIN_POLL_INTERVAL_SEC}, {MAX_POLL_INTERVAL_SEC}] for '{item.market_hash_name}'")
            poll_interval = item.poll_interval_sec

        # Resolve nameid server-side for streams that need it (never client-supplied).
        item_nameid = None
        if item.stream in NAMEID_REQUIRED_STREAMS:
            item_nameid = resolve_item_nameid(item.market_hash_name)
            if item_nameid is None:
                reject_and_log(400, f"Couldn't find '{item.market_hash_name}' on Steam — no item id resolvable, which the '{item.stream}' stream requires.")

        normalized.append({
            "market_hash_name": item.market_hash_name,
            "appid": item.appid,
            "item_nameid": item_nameid,
            "stream": item.stream,
            "currency": item.currency,
            "country": item.country,
            "language": item.language,
            "poll_interval_sec": poll_interval,
        })

    print(f"\n📥 PUT /tracked-items: replacing set with {len(normalized)} item(s)")

    # --- feasibility gate over the WHOLE desired live set (one check) ---
    # pricehistory is excluded (fixed hourly tick, off the sustained budget),
    # same rule as POST/boot validation.
    rate_limit, window = read_rate_budget()
    intervals = [n["poll_interval_sec"] for n in normalized if n["stream"] != "pricehistory"]
    ok, total, util = compute_feasibility(rate_limit, window, intervals)
    if not ok:
        reject_and_log(409, f"Desired set would exceed rate limit: {total} req/{window}s vs budget {rate_limit}. Drop an item or raise poll_interval_sec.")

    # --- apply atomically: upsert+enable desired, disable everything else ---
    names = [n["market_hash_name"] for n in normalized]
    streams = [n["stream"] for n in normalized]
    async with holder.pool.acquire() as conn:
        async with conn.transaction():
            for n in normalized:
                await conn.execute(
                    """
                    INSERT INTO tracked_items
                        (market_hash_name, appid, item_nameid, stream,
                         currency, country, language, poll_interval_sec, enabled)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
                    ON CONFLICT (market_hash_name, stream) DO UPDATE SET
                        appid = EXCLUDED.appid,
                        item_nameid = EXCLUDED.item_nameid,
                        currency = EXCLUDED.currency,
                        country = EXCLUDED.country,
                        language = EXCLUDED.language,
                        poll_interval_sec = EXCLUDED.poll_interval_sec,
                        enabled = TRUE
                    """,
                    n["market_hash_name"], n["appid"], n["item_nameid"], n["stream"],
                    n["currency"], n["country"], n["language"], n["poll_interval_sec"],
                )
            # Disable every currently-enabled row not in the desired set. With an
            # empty desired set the arrays are empty, NOT IN () is TRUE, so all
            # enabled rows are disabled — the "track nothing" case.
            disabled = await conn.fetch(
                """
                UPDATE tracked_items SET enabled = FALSE
                WHERE enabled = TRUE
                  AND (market_hash_name, stream) NOT IN (
                      SELECT * FROM unnest($1::text[], $2::text[])
                  )
                RETURNING market_hash_name, stream
                """,
                names, streams,
            )

    await mirror_config_after_write()
    disabled_keys = [{"market_hash_name": r["market_hash_name"], "stream": r["stream"]} for r in disabled]
    print(f"  ✓ tracked set replaced: {len(normalized)} enabled, {len(disabled_keys)} disabled "
          f"({total} req/{window}s, {util:.1f}% capacity) — reconcile chain applies it")
    return {
        "status": "replaced",
        "enabled": [{"market_hash_name": n["market_hash_name"], "stream": n["stream"]} for n in normalized],
        "disabled": disabled_keys,
        "capacity": {
            "total_reqs": total,
            "window_seconds": window,
            "budget": rate_limit,
            "utilization_pct": round(util, 1),
        },
    }


# ---------------------------------------------------------------------------
# WebSocket live push — the delta layer on top of the REST cold-start.
#
# One multiplexed socket per client carries many (name, stream) subscriptions
# (browsers cap ~6 connections/host, so cards share one socket). The client
# subscribes per card; the server pushes the freshly-written row whenever the
# NOTIFY listener sees an insert for a subscribed (name, stream).
#
# Message contract:
#   client -> server: {"action": "subscribe"|"unsubscribe", "name": ..., "stream": ...}
#   server -> client: {"type": "update", "stream": ..., "name": ..., "data": <model>}
#   data shape per stream == the matching REST GET (OverviewResponse |
#   BookSnapshot | ActivityResponse | HistoryResponse). For append streams it's
#   the latest single point; histogram/activity carry the full latest snapshot.
#
# On subscribe the server immediately pushes the current latest-1 (a WS-native
# cold-start), so a freshly opened card paints without a separate REST call.
# ---------------------------------------------------------------------------


@app.websocket("/ws")
async def market_data_websocket(websocket: WebSocket):
    """Multiplexed live feed: client subscribes to (name, stream) pairs and
    receives the fresh row on every matching write.

    No Origin/auth gate — same trust model as the REST endpoints this pass. A
    bad stream or missing field is answered with an inline error frame rather
    than closing the socket, so one malformed message doesn't drop the others.
    """
    await websocket.accept()
    try:
        while True:
            msg = await websocket.receive_json()
            action = msg.get("action")
            name = msg.get("name")
            stream = msg.get("stream")

            if action not in ("subscribe", "unsubscribe") or not name or not stream:
                await websocket.send_json(
                    {"type": "error", "detail": "expected {action, name, stream}"}
                )
                continue
            if stream not in VALID_STREAMS:
                await websocket.send_json(
                    {"type": "error", "detail": f"invalid stream '{stream}'"}
                )
                continue

            if action == "unsubscribe":
                await ws_registry.unsubscribe(name, stream, websocket)
                continue

            # subscribe: register, then push the current latest-1 immediately so
            # the card paints without a REST round-trip.
            await ws_registry.subscribe(name, stream, websocket)
            async with holder.pool.acquire() as conn:
                data = await read_ws_delta_for_stream(conn, name, stream)
            if data is not None:
                await websocket.send_text(build_update_message(name, stream, data))
    except WebSocketDisconnect:
        # Clean every subscription this socket held, in one pass.
        await ws_registry.drop_socket(websocket)
