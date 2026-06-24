"""FastAPI read-path application for the CS2 market data store.

Five read-only endpoints serving the frontend from the existing
Postgres/Timescale instance. The connection pool is opened once on startup
(lifespan) and a connection is borrowed per request. This process never
writes to the database and is independent of ingestion/the schedulers.

Run with:  uvicorn api.main:app --reload --port 8000
Docs at:   http://localhost:8000/docs
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.databasePool import holder, open_read_pool
from api.responseModels import (
    ActivityResponse,
    BookSnapshot,
    HistoryResponse,
    MAX_POLL_INTERVAL_SEC,
    MIN_POLL_INTERVAL_SEC,
    MetaResponse,
    OverviewResponse,
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
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the read-only pool once at startup, close it at shutdown."""
    holder.pool = await open_read_pool()
    try:
        yield
    finally:
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
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["*"],
)


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


# Wire each live stream to its single reader. POST/PATCH look the item's stream
# up here so they return current data via the exact same callable the GET uses.
STREAM_TO_READER.update({
    "priceoverview": read_recent_overview,
    "histogram": read_latest_orderbook,
    "activity": read_recent_activity,
})


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
        return None  # e.g. pricehistory: archival, no live cold-start read
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
    - rate_limit: the configured budget from config.yaml. "used" is NOT live —
      the limiter lives in the separate scheduler process and its in-memory
      state isn't reachable cross-process this pass. Marked used_is_live=False.
    - last_ingest: most recent write across the three live snapshot tables.
    """
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

    with open("config.yaml") as f:
        limits = yaml.safe_load(f)["LIMITS"]
    rate_limit = RateLimitState(
        used=None,
        limit=limits["REQUESTS"],
        window_seconds=limits["WINDOW_SECONDS"],
        used_is_live=False,
        note="'used' not live: rate limiter runs in the scheduler process and "
        "is not reachable cross-process yet. Showing configured budget.",
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
    """Return price history for an item, bounded by the requested range."""
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
        raise HTTPException(status_code=404, detail=f"No price history for '{name}'")

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
    """Poll intervals of the currently enabled set, optionally excluding one row
    (so a PATCH measures the set WITHOUT the row it's about to change)."""
    if exclude_id is None:
        rows = await conn.fetch("SELECT poll_interval_sec FROM tracked_items WHERE enabled = TRUE")
    else:
        rows = await conn.fetch(
            "SELECT poll_interval_sec FROM tracked_items WHERE enabled = TRUE AND id <> $1",
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
    print(f"\n📥 POST /tracked-items: '{item.market_hash_name}' | {item.stream} | "
          f"every {item.poll_interval_sec}s | cur={item.currency} country={item.country} appid={item.appid}")

    # --- validate (untrusted body) ---
    if item.stream not in VALID_STREAMS:
        reject_and_log(400, f"Invalid stream '{item.stream}'. Use one of: {', '.join(VALID_STREAMS)}")
    if not item.market_hash_name.strip():
        reject_and_log(400, "market_hash_name must not be empty")
    if item.appid <= 0:
        reject_and_log(400, f"Invalid appid {item.appid} (must be positive; 730 = CS2)")
    if item.currency <= 0:
        reject_and_log(400, f"Invalid currency id {item.currency}")
    if not (MIN_POLL_INTERVAL_SEC <= item.poll_interval_sec <= MAX_POLL_INTERVAL_SEC):
        reject_and_log(
            400,
            f"poll_interval_sec {item.poll_interval_sec} out of bounds "
            f"[{MIN_POLL_INTERVAL_SEC}, {MAX_POLL_INTERVAL_SEC}]",
        )

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
        rate_limit, window = read_rate_budget()
        intervals = await fetch_enabled_intervals(conn)  # disabled re-adds aren't in here yet
        ok, total, util = compute_feasibility(rate_limit, window, intervals + [item.poll_interval_sec])
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
            item.currency, item.country, item.language, item.poll_interval_sec,
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
    if patch.poll_interval_sec is not None and not (
        MIN_POLL_INTERVAL_SEC <= patch.poll_interval_sec <= MAX_POLL_INTERVAL_SEC
    ):
        reject_and_log(
            400,
            f"poll_interval_sec {patch.poll_interval_sec} out of bounds "
            f"[{MIN_POLL_INTERVAL_SEC}, {MAX_POLL_INTERVAL_SEC}]",
        )

    async with holder.pool.acquire() as conn:
        cur = await resolve_target_row(conn, patch.market_hash_name, patch.stream)
        item_id = cur["id"]

        new_stream = patch.new_stream if patch.new_stream is not None else cur["stream"]
        new_interval = patch.poll_interval_sec if patch.poll_interval_sec is not None else cur["poll_interval_sec"]
        new_enabled = patch.enabled if patch.enabled is not None else cur["enabled"]

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
        # disabled). Otherwise it frees budget -> skip the check.
        rate_limit, window = read_rate_budget()
        old_contrib = (window // cur["poll_interval_sec"]) if cur["enabled"] else 0
        new_contrib = (window // new_interval) if new_enabled else 0
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
