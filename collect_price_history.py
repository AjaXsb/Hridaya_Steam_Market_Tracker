"""
Standalone data collector for populating the database with price history.

This script:
1. Loads items from data/cs2_item_ids.json
2. Fetches price history for each item via Steam API
3. Stores results in the SQLite database

Usage:
    python collect_price_history.py
    python collect_price_history.py --skip 150   # Resume from item 151

Note: This script manages its own RateLimiter instance (separate from cerebro.py).
"""

import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from src.RateLimiter import RateLimiter
from src.steamAPIclient import SteamAPIClient
from src.SQLinserts import SQLinserts

# Canonical currency we *request* from Steam. The pricehistory endpoint ignores
# this and returns the logged-in wallet's currency anyway; the real currency is
# derived from the response and tagged on each row by SQLinserts. USD for now.
CANONICAL_CURRENCY = 1  # USD


async def collect_price_history(skip: int = 0, refresh: bool = False, fresh_days: float = 1.0):
    """Fetch and store price history for all items in cs2_item_ids.json.

    Skip logic (DB is the source of truth, survives crashes):
      - Item with NO rows           -> fetch (backfill).
      - Item whose newest point is
        older than fresh_days        -> fetch (top up; per-point delta dedup only
                                        inserts the new tail, so it's cheap).
      - Item fresh within fresh_days -> skip (no API call).

    This makes an interrupted run resume where it left off AND keeps stale
    (months-old) items updating instead of being skipped forever. Pass
    refresh=True to re-fetch everything regardless of freshness.
    """

    # Load items from JSON
    items_path = Path("data/cs2_item_ids.json")
    if not items_path.exists():
        print(f"Error: {items_path} not found")
        return

    with open(items_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    total_items = len(items)
    print(f"Loaded {total_items} items from {items_path}")

    if skip > 0:
        print(f"Skipping first {skip} items (resuming from item {skip + 1})")

    # Create rate limiter (15 requests per 60 seconds - Steam's limit)
    rate_limiter = RateLimiter(max_requests=15, window_seconds=60.0)
    print(f"RateLimiter created (15 req/60s)")

    # Track progress
    successful = 0
    failed = 0
    skipped = 0
    failed_items = []

    # Backend: Postgres/Timescale only, DSN from env. No SQLite fallback.
    load_dotenv()
    timescale_dsn = os.getenv("CS2_PG_DSN")
    if not timescale_dsn:
        print("Error: CS2_PG_DSN not set (required; no SQLite fallback). Set it in .env")
        return
    async with SteamAPIClient(rate_limiter=rate_limiter) as client, SQLinserts(timescale_dsn=timescale_dsn) as db:
        print("Database: Postgres/Timescale (CS2_PG_DSN)")

        # Auto-resume: skip items whose newest point is still fresh (no API call).
        # Stale items (older than fresh_days) are re-fetched to top up. --refresh
        # bypasses the check entirely and re-fetches everything.
        last_timestamps = {}
        fresh_cutoff = datetime.now() - timedelta(days=fresh_days)
        if not refresh:
            last_timestamps = await db.fetch_price_history_last_timestamps()
            fresh_count = sum(1 for t in last_timestamps.values() if t and t >= fresh_cutoff)
            if last_timestamps:
                print(f"{len(last_timestamps)} items in DB, {fresh_count} fresh "
                      f"(<{fresh_days}d) — those will be skipped, stale ones re-fetched")

        print("=" * 60)
        print("Starting collection...\n")

        for index, (market_hash_name, item_nameid) in enumerate(items.items(), start=1):
            # Skip items if resuming
            if index <= skip:
                continue

            # Auto-resume: skip only items whose newest point is still fresh.
            # Missing (never fetched) or stale items fall through and get fetched.
            last_time = last_timestamps.get(market_hash_name)
            if last_time and last_time >= fresh_cutoff:
                skipped += 1
                continue
            # Build item config for storage. Note: currency is NOT set here — the
            # real currency is derived from Steam's response and tagged per row by
            # SQLinserts, so hardcoding it would mislabel non-USD wallet data.
            item_config = {
                "market_hash_name": market_hash_name,
                "appid": 730,  # CS2
                "country": "US",
                "language": "english",
                "item_nameid": item_nameid
            }

            try:
                # Fetch price history
                data = await client.fetch_price_history(
                    appid=730,
                    market_hash_name=market_hash_name,
                    currency=CANONICAL_CURRENCY,
                    country="US",
                    language="english"
                )

                # Store in database
                await db.store_data(data, item_config)

                successful += 1
                # Item now has a today-dated point -> fresh -> auto-skipped on a
                # re-run. This [index/total] is the resume checkpoint: on a crash,
                # plain re-run skips everything up to here automatically.
                print(f"[{index}/{total_items}] ✓ {market_hash_name} (fresh)")

            except Exception as e:
                failed += 1
                failed_items.append((market_hash_name, str(e)))
                print(f"[{index}/{total_items}] ✗ {market_hash_name}: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("Collection complete!")
    print(f"  Successful: {successful}")
    print(f"  Skipped (already ingested): {skipped}")
    print(f"  Failed: {failed}")

    if failed_items:
        print(f"\nFailed items:")
        for name, error in failed_items[:10]:  # Show first 10
            print(f"  - {name}: {error}")
        if len(failed_items) > 10:
            print(f"  ... and {len(failed_items) - 10} more")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect price history for CS2 items")
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Number of items to skip (resume from item N+1)"
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-fetch all items regardless of freshness (ignores the staleness window)"
    )
    parser.add_argument(
        "--fresh-days",
        type=float,
        default=1.0,
        help="Skip an item only if its newest stored point is within this many days (default: 1)"
    )
    args = parser.parse_args()

    try:
        asyncio.run(collect_price_history(skip=args.skip, refresh=args.refresh, fresh_days=args.fresh_days))
    except KeyboardInterrupt:
        print("\n\nCollection interrupted by user")
    except Exception as e:
        print(f"\nFatal error: {e}")
        raise
