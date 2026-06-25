"""One-time migration: seed the tracked_items table from config.yaml.

tracked_items becomes the single source of truth for what is tracked and how
(stream, currency, country, language, cadence). config.yaml stays in the repo
untouched as the seed source / fallback reference.

Same spirit as the price-history migration: read the existing config, insert
one row per tracked item/stream, then verify the seeded rows match config
exactly. Re-running is safe — the unique (market_hash_name, stream) constraint
makes inserts idempotent via ON CONFLICT DO NOTHING.

Run:  python seed_tracked_items.py
"""

import asyncio
import os

import asyncpg
from dotenv import load_dotenv

from utility.loadConfig_utility import load_config_from_yaml

# config api_id  ->  tracked_items.stream. All four streams live in tracked_items;
# pricehistory is the hourly-archival one (clockwork), the other three are live
# snapshot pollers (snoozer).
API_ID_TO_STREAM = {
    "priceoverview": "priceoverview",
    "itemordershistogram": "histogram",
    "itemordersactivity": "activity",
    "pricehistory": "pricehistory",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tracked_items (
    id BIGSERIAL PRIMARY KEY,
    market_hash_name TEXT NOT NULL,
    appid INTEGER NOT NULL,
    item_nameid BIGINT,
    stream TEXT NOT NULL,
    currency INTEGER NOT NULL,
    country TEXT NOT NULL DEFAULT 'US',
    language TEXT NOT NULL DEFAULT 'english',
    poll_interval_sec INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market_hash_name, stream)
)
"""


def build_rows_from_config(config: dict) -> list[dict]:
    """Turn config TRACKING_ITEMS into tracked_items rows."""
    rows = []
    for item in config["TRACKING_ITEMS"]:
        stream = API_ID_TO_STREAM.get(item["api_id"])
        if stream is None:
            # unknown api_id — not a tracked stream
            continue
        rows.append(
            {
                "market_hash_name": item["market_hash_name"],
                "appid": item["appid"],
                "item_nameid": item.get("item_nameid"),
                "stream": stream,
                "currency": item.get("currency", 1),
                "country": item.get("country", "US"),
                "language": item.get("language", "english"),
                "poll_interval_sec": item["polling-interval-in-seconds"],
            }
        )
    return rows


async def seed():
    load_dotenv()
    dsn = os.getenv("CS2_PG_DSN")
    if not dsn:
        print("Error: CS2_PG_DSN not set (required). Set it in .env")
        return

    config = load_config_from_yaml("config.yaml")
    rows = build_rows_from_config(config)
    print(f"Config yields {len(rows)} tracked item/stream rows to seed.")

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(CREATE_TABLE_SQL)

        inserted = 0
        for r in rows:
            status = await conn.execute(
                """
                INSERT INTO tracked_items
                    (market_hash_name, appid, item_nameid, stream,
                     currency, country, language, poll_interval_sec)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (market_hash_name, stream) DO NOTHING
                """,
                r["market_hash_name"], r["appid"], r["item_nameid"], r["stream"],
                r["currency"], r["country"], r["language"], r["poll_interval_sec"],
            )
            if status.endswith("1"):
                inserted += 1
        print(f"Inserted {inserted} new row(s) "
              f"({len(rows) - inserted} already present, skipped).")

        # ---- Verify seeded rows match config exactly ----
        db_rows = await conn.fetch(
            "SELECT market_hash_name, stream, currency, country, language, "
            "poll_interval_sec, item_nameid FROM tracked_items"
        )
        db_set = {
            (d["market_hash_name"], d["stream"], d["currency"], d["country"],
             d["language"], d["poll_interval_sec"], d["item_nameid"])
            for d in db_rows
        }
        cfg_set = {
            (r["market_hash_name"], r["stream"], r["currency"], r["country"],
             r["language"], r["poll_interval_sec"], r["item_nameid"])
            for r in rows
        }
        missing = cfg_set - db_set       # in config but not in DB
        extra = db_set - cfg_set         # in DB but not in config

        print("\n=== Seed verification (table vs config) ===")
        print(f"  config rows: {len(cfg_set)}   db rows: {len(db_set)}")
        if not missing and not extra:
            print("  ✓ EXACT MATCH — every config item/stream/interval present, no extras")
        else:
            if missing:
                print(f"  ✗ MISSING from DB: {missing}")
            if extra:
                print(f"  ⚠ EXTRA in DB (not in current config): {extra}")

        print("\n  Seeded rows:")
        for d in sorted(db_rows, key=lambda x: (x["market_hash_name"], x["stream"])):
            print(f"    {d['market_hash_name']:<45} {d['stream']:<13} "
                  f"every {d['poll_interval_sec']}s  cur={d['currency']} "
                  f"{d['country']}/{d['language']}  nameid={d['item_nameid']}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(seed())
