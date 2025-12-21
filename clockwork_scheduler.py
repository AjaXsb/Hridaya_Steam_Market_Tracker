"""
Clockwork Scheduler - Fixed-time scheduling for historical price data.

This scheduler runs pricehistory API calls at :10 past every UTC hour,
since Steam only updates historical data once per hour.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List
from steamAPIclient import SteamAPIClient
from loadConfig import load_config_from_yaml


class ClockworkScheduler:                                                                                   
    """
    Executes pricehistory API calls on a fixed hourly schedule.

    Runs at 10 minutes past every UTC hour (e.g., 00:10, 01:10, 02:10, etc.)
    since Steam's price history data only updates once per hour.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the clockwork scheduler.

        Args:
            config_path: Path to the YAML configuration file
        """
        self.config = load_config_from_yaml(config_path)
        self.history_items = self._load_history_items()
        self.steam_client = None  # Will be initialized in run()

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
        Calculate the next execution time (:10 past the next hour).

        Returns:
            Datetime of next execution (next hour at :10 UTC)
        """
        # Get current UTC time
        now = datetime.now(timezone.utc)

        # Start with :10 of current hour
        next_run = now.replace(minute=10, second=0, microsecond=0)

        # If we're past :10, move to next hour
        if now.minute >= 10:
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
        """
        print(f"[{datetime.now()}] Executing hourly price history updates")

        for item in self.history_items:
            try:
                result = await self.steam_client.fetch_price_history(
                    appid=item['appid'],
                    market_hash_name=item['market_hash_name']
                )

                item['last_update'] = datetime.now()

                # TODO: Store result to database/file
                print(f"  ✓ {item['market_hash_name']}: {len(result.prices)} data points")

            except Exception as e:
                print(f"  ✗ {item['market_hash_name']}: Error - {e}")

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
        2. Calculate next :10 past the hour
        3. Sleep until that time
        4. Execute all pricehistory items
        5. Repeat from step 2
        """
        async with SteamAPIClient() as client:
            self.steam_client = client
            print(f"Clockwork Scheduler started with {len(self.history_items)} items")

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
