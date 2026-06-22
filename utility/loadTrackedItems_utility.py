"""Read tracked items from the tracked_items table (the source of truth).

tracked_items replaces config.yaml as the authority on what is tracked and how.
This module reads enabled rows and shapes them into the same dicts the
schedulers already consume from config — so the scheduler behaves identically,
just sourced from the table.

Startup-read only this pass: no hot-reload, no NOTIFY. That arrives with the
write-path pass.
"""

import asyncpg

# tracked_items.stream  ->  the api_id the schedulers dispatch on.
# Inverse of the mapping the seed migration uses.
STREAM_TO_API_ID = {
    "priceoverview": "priceoverview",
    "histogram": "itemordershistogram",
    "activity": "itemordersactivity",
}

# Steam currency id -> ISO 4217 code, for display. Mirrors the ids the
# ingestion side requests (USD=1, GBP=2, EUR=3, INR=24).
STEAM_CURRENCY_ID_TO_ISO = {
    1: "USD",
    2: "GBP",
    3: "EUR",
    24: "INR",
}


async def fetch_enabled_tracked_items(dsn: str) -> list[dict]:
    """Return enabled tracked items shaped exactly like config TRACKING_ITEMS.

    Keys match what snoozerScheduler/execute_item read: market_hash_name,
    appid, api_id, item_nameid, currency (Steam int id), country, language,
    polling-interval-in-seconds.

    WRITE-PATH NOTE (next pass, not now): this reader does NOT resolve
    item_nameid — it trusts whatever the table holds. Seeded rows carry a
    nameid because the seed migration ran the id utility first. When the
    write-path lets rows be added directly to the table, the item-id utility
    must run against the table set to fill item_nameid for new histogram/
    activity rows; otherwise they arrive with nameid=None and break the fetch
    (and validate_required_fields' `'item_nameid' in item` presence check
    passes for None, so it won't catch them — the resolver must).
    """
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT market_hash_name, appid, item_nameid, stream,
                   currency, country, language, poll_interval_sec
            FROM tracked_items
            WHERE enabled = TRUE
            ORDER BY market_hash_name, stream
            """
        )
    finally:
        await conn.close()

    items = []
    for r in rows:
        items.append(
            {
                "market_hash_name": r["market_hash_name"],
                "appid": r["appid"],
                "api_id": STREAM_TO_API_ID[r["stream"]],
                "item_nameid": r["item_nameid"],
                "currency": r["currency"],
                "country": r["country"],
                "language": r["language"],
                "polling-interval-in-seconds": r["poll_interval_sec"],
            }
        )
    return items
