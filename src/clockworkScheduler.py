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
        config_path: str = "config.yaml"
    ):
        """
        Initialize the clockwork scheduler.

        Args:
            items: Optional list of items to track. If None, loads from config.
            rate_limiter: Optional shared RateLimiter instance. If None, client creates its own.
            config_path: Path to the YAML configuration file (used if items is None)
        """
        self.rate_limiter = rate_limiter

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
            if item['apiid'] == 'pricehistory':
                item['last_update'] = None
                history_items.append(item)

        return history_items

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
        async with SteamAPIClient(rate_limiter=self.rate_limiter) as client, SQLinserts() as wizard:
            self.steam_client = client
            self.data_wizard = wizard
            print(f"Clockwork Scheduler started with {len(self.history_items)} items")
            print(f"Database: SQLite at market_data.db")
            if self.rate_limiter is not None:
                print(f"Using shared RateLimiter (orchestrated mode)")

            # Run once immediately
            await self.run_initial_fetch()

            while True:
                next_execution = self.get_next_execution_time()
                sleep_seconds = self.calculate_sleep_duration(next_execution)
                print(f"  Sleeping until {next_execution.strftime('%H:%M:%S')} UTC ({sleep_seconds:.0f} seconds)")
                await asyncio.sleep(sleep_seconds)
                await self.execute_history_items()


# Entry point for testing
if __name__ == "__main__":
    scheduler = ClockworkScheduler()
    asyncio.run(scheduler.run())
