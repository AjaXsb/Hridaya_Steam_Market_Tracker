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
from pathlib import Path
from src.RateLimiter import RateLimiter
from src.steamAPIclient import SteamAPIClient
from src.SQLinserts import SQLinserts

# Canonical currency we *request* from Steam. The pricehistory endpoint ignores
# this and returns the logged-in wallet's currency anyway; the real currency is
# derived from the response and tagged on each row by SQLinserts. USD for now.
CANONICAL_CURRENCY = 1  # USD


async def collect_price_history(skip: int = 0):
    """Fetch and store price history for all items in cs2_item_ids.json."""

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
    failed_items = []

    async with SteamAPIClient(rate_limiter=rate_limiter) as client, SQLinserts() as db:
        print(f"Database: SQLite at data/market_data.db")
        print("=" * 60)
        print("Starting collection...\n")

        for index, (market_hash_name, item_nameid) in enumerate(items.items(), start=1):
            # Skip items if resuming
            if index <= skip:
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
                print(f"[{index}/{total_items}] ✓ {market_hash_name}")

            except Exception as e:
                failed += 1
                failed_items.append((market_hash_name, str(e)))
                print(f"[{index}/{total_items}] ✗ {market_hash_name}: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("Collection complete!")
    print(f"  Successful: {successful}")
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
    args = parser.parse_args()

    try:
        asyncio.run(collect_price_history(skip=args.skip))
    except KeyboardInterrupt:
        print("\n\nCollection interrupted by user")
    except Exception as e:
        print(f"\nFatal error: {e}")
        raise
