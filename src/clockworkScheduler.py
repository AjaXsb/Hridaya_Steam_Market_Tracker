"""
Clockwork Scheduler - Fixed-time scheduling for historical price data.

This scheduler runs pricehistory API calls at :30 past every UTC hour,
since Steam only updates historical data once per hour (with some lag).
"""

import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from src.steamAPIclient import SteamAPIClient
from src.RateLimiter import RateLimiter
from utility.loadConfig_utility import load_config_from_yaml
from src.SQLinserts import SQLinserts


class ClockworkScheduler:
    """
    Executes pricehistory API calls on a fixed hourly schedule.

    Runs at 30 minutes past every UTC hour (e.g., 00:30, 01:30, 02:30, etc.)
    since Steam's price history data updates hourly with ~20-30 min lag.
    """

    def __init__(
        self,
        items: Optional[List[dict]] = None,
        rate_limiter: Optional[RateLimiter] = None,
        config_path: str = "config.yaml",
        timescale_dsn: Optional[str] = None
    ):
        """
        Initialize the clockwork scheduler.

        Args:
            items: Optional list of items to track. If None, loads from config.
            rate_limiter: Optional shared RateLimiter instance. If None, client creates its own.
            config_path: Path to the YAML configuration file (used if items is None)
            timescale_dsn: Optional Postgres/Timescale DSN. When set, SQLinserts
                writes to Postgres; when None it falls back to SQLite.
        """
        self.rate_limiter = rate_limiter
        self.timescale_dsn = timescale_dsn

        if items is not None:
            self.history_items = items
            # Initialize tracking fields for each item
            for item in self.history_items:
                item['last_update'] = None
        else:
            self.config = load_config_from_yaml(config_path)
            self.history_items = self._load_history_items()

        self.steam_client: Optional[SteamAPIClient] = None  # Will be initialized in run()
        self.data_wizard: Optional[SQLinserts] = None  # Will be initialized in run()

    def _load_history_items(self) -> List[dict]:
        """
        Load all pricehistory items from config.

        Returns:
            List of pricehistory item configurations
        """
        history_items = []
        for item in self.config['TRACKING_ITEMS']:
            if item['api_id'] == 'pricehistory':
                item['last_update'] = None
                history_items.append(item)

        return history_items

    def reconcile_history_set(self, new_items: List[dict]) -> dict:
        """Swap the archival item set to a new desired set, live, no restart.

        The clockwork mirror of snoozer.reconcile_live_set. This scheduler is a
        single loop over self.history_items fired on the hourly tick (not one
        task per item), so reconciling means atomically rebinding that list — the
        sleeping run loop picks up the new list on its next tick.

        Surviving items keep last_update (keyed by market_hash_name+api_id, the
        UNIQUE table key). Brand-new items get last_update=None and are returned
        in added_items so the caller can fetch them immediately rather than make
        the user wait up to an hour for the next tick.

        Returns a diff summary; added_items carries the new item dicts for the
        immediate-fetch path.
        """
        prev = {(i['market_hash_name'], i['api_id']): i for i in self.history_items}
        new_keys = {(i['market_hash_name'], i['api_id']) for i in new_items}

        rebuilt = []
        added_items = []
        for item in new_items:
            key = (item['market_hash_name'], item['api_id'])
            old = prev.get(key)
            if old is not None:
                item['last_update'] = old.get('last_update')
            else:
                item['last_update'] = None  # brand new -> fetch asap
                added_items.append(item)
            rebuilt.append(item)

        added = sorted(new_keys - prev.keys())
        removed = sorted(prev.keys() - new_keys)

        # Atomic rebind — the running loop sees the new list next tick.
        self.history_items = rebuilt
        return {
            "added": added,
            "removed": removed,
            "total": len(rebuilt),
            "added_items": added_items,
        }

    async def fetch_items_now(self, items: List[dict]) -> None:
        """Fetch the given history items immediately (off the hourly schedule).

        Used right after a reconcile so a newly-added pricehistory item gets its
        full series seeded within seconds instead of waiting for the next :30
        tick — the archival parallel to snoozer firing a new item at urgency=inf.
        No-op until the steam client exists (run() sets it), so an item added
        before clockwork's run() starts is simply caught by run_initial_fetch.
        """
        if not items or self.steam_client is None:
            return
        print(f"  ↪ immediate price-history fetch for {len(items)} new item(s)")
        for item in items:
            await self._fetch_item_with_retry(item)

    def get_next_execution_time(self) -> datetime:
        """
        Calculate the next execution time (:30 past the next hour).

        Returns:
            Datetime of next execution (next hour at :30 UTC)
        """
        # Get current UTC time
        now = datetime.now(timezone.utc)

        # Start with :30 of current hour
        next_run = now.replace(minute=30, second=0, microsecond=0)

        # If we're past :30, move to next hour
        if now.minute >= 30:
            next_run = next_run + timedelta(hours=1)

        return next_run

    def calculate_sleep_duration(self, next_execution: datetime) -> float:
        """
        Calculate seconds to sleep until next execution.

        Args:
            next_execution: Target execution datetime

        Returns:
            Sleep duration in seconds
        """
        time_until = next_execution - datetime.now(timezone.utc)
        return time_until.total_seconds()

    async def execute_history_items(self) -> None:
        """
        Execute pricehistory API calls for all configured items.

        This runs all history items in sequence, respecting the rate limiter.
        Retries transient errors (429, 5xx, network) with exponential backoff.
        """
        print(f"[{datetime.now()}] Executing hourly price history updates")

        for item in self.history_items:
            await self._fetch_item_with_retry(item)

    async def _fetch_item_with_retry(self, item: dict, max_retries: int = 4) -> None:
        """
        Fetch price history for a single item with retry logic.
        
        Retries transient errors (429, 5xx, network) and auth errors (400, 401, 403)
        with backoff. Auth errors are retried because cookies can be hot-swapped in .env.
        
        Args:
            item: Item configuration to fetch
            max_retries: Maximum retry attempts for transient errors
        """
        backoff_seconds = [30, 60, 120, 240]  # Backoff delays for each retry
        
        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                result = await self.steam_client.fetch_price_history(
                    appid=item['appid'],
                    market_hash_name=item['market_hash_name'],
                    currency=item.get('currency', 1),
                    country=item.get('country', 'US'),
                    language=item.get('language', 'english')
                )

                # Store result to database (prints its own status)
                await self.data_wizard.store_data(result, item)
                item['last_update'] = datetime.now()
                return  # Success, exit retry loop

            except aiohttp.ClientResponseError as e:
                if e.status == 429 or e.status >= 500:
                    # Transient error - retry with backoff
                    if attempt < max_retries:
                        delay = backoff_seconds[attempt]
                        error_type = "Rate limited" if e.status == 429 else f"Server error {e.status}"
                        print(f"  ⏸ {item['market_hash_name']}: {error_type} - retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                    else:
                        print(f"  ✗ {item['market_hash_name']}: Failed after {max_retries} retries ({e.status})")
                elif e.status in (400, 401, 403):
                    # Auth/cookie error - retry with backoff (cookies can be hot-swapped)
                    if attempt < max_retries:
                        delay = backoff_seconds[attempt]
                        print(f"  ⏸ {item['market_hash_name']}: HTTP {e.status} (cookie error?) - update .env, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                    else:
                        print(f"  ✗ {item['market_hash_name']}: HTTP {e.status} after {max_retries} retries - check Steam cookies in .env")
                else:
                    # Other 4xx - no retry
                    print(f"  ✗ {item['market_hash_name']}: HTTP {e.status}: {e.message}")
                    return
            
            except aiohttp.ClientError as e:
                # Network error - retry with backoff
                if attempt < max_retries:
                    delay = backoff_seconds[attempt]
                    print(f"  ⏸ {item['market_hash_name']}: Network error - retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    print(f"  ✗ {item['market_hash_name']}: Network error after {max_retries} retries - {e}")
            
            except Exception as e:
                # Unexpected error - no retry
                print(f"  ✗ {item['market_hash_name']}: Error - {e}")
                return

    async def run_initial_fetch(self) -> None:
        """
        Run pricehistory once immediately when scheduler starts.

        This ensures we have data right away, then we switch to hourly schedule.
        """
        print("Running initial price history fetch...")
        await self.execute_history_items()

    async def run(self) -> None:
        """
        Main clockwork loop.

        Algorithm:
        1. Run pricehistory immediately on startup
        2. Calculate next :30 past the hour
        3. Sleep until that time
        4. Execute all pricehistory items
        5. Repeat from step 2
        """
        async with SteamAPIClient(rate_limiter=self.rate_limiter) as client, SQLinserts(timescale_dsn=self.timescale_dsn) as wizard:
            self.steam_client = client
            self.data_wizard = wizard

            # Run once immediately
            await self.run_initial_fetch()

            while True:
                next_execution = self.get_next_execution_time()
                sleep_seconds = self.calculate_sleep_duration(next_execution)
                print(f"  Historical collector sleeping until {next_execution.strftime('%H:%M:%S')} UTC ({sleep_seconds:.0f} seconds)")
                await asyncio.sleep(sleep_seconds)
                await self.execute_history_items()


# Entry point for testing
if __name__ == "__main__":
    scheduler = ClockworkScheduler()
    asyncio.run(scheduler.run())
