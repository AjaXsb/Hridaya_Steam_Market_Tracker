"""Market-data NOTIFY plumbing: emit a signal whenever a new data row lands.

Parallel to the tracked_items trigger in configTableSync_utility, but for the
four DATA tables instead of the tracked set. Every successful insert into a
data table fires a thin NOTIFY on one shared channel; the API process LISTENs
and pushes the fresh row to subscribed WebSockets.

Design choices (settled in design discussion):
  * ONE channel for all four streams (`market_data`); the payload carries the
    stream, so the API routes on the payload, not the channel — one listener,
    one handler, mirroring the single tracked_items_changed channel.
  * Thin payload: {name, stream} only. The NOTIFY payload caps at ~8000 bytes,
    far too small for an order book (tens of KB of JSONB graphs), so the signal
    never carries data — it just says "a new row for this item+stream exists".
    The API re-reads the latest row and ships the fat payload over the WebSocket
    (no size cap there).
  * DB trigger as the single emit point (DB-side), so any insert — cerebro or a
    manual SQL write — produces the same signal.

Installed once at schema setup time from SQLinserts._initialize_timescale.
"""

import asyncpg

# The single channel all four data streams emit on. The API runs LISTEN on this
# one name and switches on the payload's `stream` field.
MARKET_DATA_CHANNEL = "market_data"

# Maps each data table to the stream label stamped into its NOTIFY payload, so
# the API's stream->reader dispatch matches the tracked_items.stream vocabulary
# (priceoverview/histogram/activity/pricehistory).
DATA_TABLE_TO_STREAM = {
    "price_overview": "priceoverview",
    "orders_histogram": "histogram",
    "orders_activity": "activity",
    "price_history": "pricehistory",
}


async def install_market_data_notify_trigger(conn: asyncpg.Connection) -> None:
    """Create the AFTER INSERT trigger that pg_notifies on a new data row.

    One shared trigger function serves all four tables; each CREATE TRIGGER
    passes its stream label as TG_ARGV[0], so the function stamps the right
    stream into the payload without a table-name lookup. Idempotent
    (CREATE OR REPLACE + DROP TRIGGER IF EXISTS), so re-running schema setup is
    safe.

    Takes an existing connection (the caller already holds the ingestion pool),
    unlike the tracked_items installer which opens its own — this runs inside
    _initialize_timescale where a pooled connection is already in hand.

    Both schedulers (snoozer + clockwork) run _initialize_timescale
    concurrently, and CREATE OR REPLACE FUNCTION is NOT race-safe (two concurrent
    replaces collide on the pg_proc catalog -> duplicate-key). A transaction-
    level advisory lock serializes the install so the second caller waits and
    then no-ops cleanly. The arbitrary key is shared by all callers.
    """
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(841_730_001)")
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION notify_market_data()
            RETURNS trigger AS $$
            DECLARE
                payload TEXT;
            BEGIN
                payload := json_build_object(
                    'name', NEW.market_hash_name,
                    'stream', TG_ARGV[0]
                )::text;
                PERFORM pg_notify('{MARKET_DATA_CHANNEL}', payload);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """
        )

        for table, stream in DATA_TABLE_TO_STREAM.items():
            trigger_name = f"{table}_notify_trg"
            await conn.execute(
                f"""
                DROP TRIGGER IF EXISTS {trigger_name} ON {table};
                CREATE TRIGGER {trigger_name}
                    AFTER INSERT ON {table}
                    FOR EACH ROW EXECUTE FUNCTION notify_market_data('{stream}');
                """
            )
