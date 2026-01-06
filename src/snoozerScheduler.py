"""
Live Data Scheduler - Urgency-based scheduling for real-time market data.

This scheduler manages priceoverview, itemordershistogram, and itemordersactivity
endpoints based on their latency requirements using an urgency scoring system.
"""

import asyncio
from datetime import datetime, timedelta
import aiohttp
from typing import List, Optional
from src.steamAPIclient import SteamAPIClient
from src.RateLimiter import RateLimiter
from utility.loadConfig_utility import load_config_from_yaml
from src.SQLinserts import SQLinserts


class snoozerScheduler:
    """
    Schedules live API calls based on urgency (how overdue each item is).

    Items with urgency >= 1.0 are overdue and need immediate execution.
    The item with highest urgency is always executed first.
    """

    def __init__(
        self,
        live_items: Optional[List[dict]] = None,
        rate_limiter: Optional[RateLimiter] = None,
        config_path: str = "config.yaml"
    ):
        """
        Initialize the live scheduler.

        Args:
            live_items: Optional list of items to track. If None, loads from config.
            rate_limiter: Optional shared RateLimiter instance. If None, client creates its own.
            config_path: Path to the YAML configuration file (used if live_items is None)
        """
        self.rate_limiter = rate_limiter

        if live_items is not None:
            self.live_items = live_items
            # Initialize tracking fields for each item
            for item in self.live_items:
                item['last_update'] = None
        else:
            self.config = load_config_from_yaml(config_path)
            self.live_items = self.load_live_items()

        self.steam_client: Optional[SteamAPIClient] = None  # Will be initialized in run()
        self.data_wizard: Optional[SQLinserts] = None  # Will be initialized in run()

    def load_live_items(self) -> List[dict]:
        """
        Load all items from config that are NOT pricehistory.

        Returns:
            List of live item configurations (priceoverview, histogram, activity)
        """
        live_items = []
        for item in self.config['TRACKING_ITEMS']:
            if item['apiid'] != 'pricehistory':
                # Initialize tracking fields
                item['last_update'] = None
                live_items.append(item)

        return live_items

    def calculate_urgency(self, item: dict) -> float:
        """
        Calculate urgency score for an item.

        Urgency = (time since last update) / (target polling rate)
        
        Returns 0.0 if item is in cooldown.

        Args:
            item: Item configuration with last_update and polling-interval-in-seconds

        Returns:
            Urgency score (>= 1.0 means overdue, < 1.0 means not yet, 0.0 if cooling down)
        """
        # If in backoff cooldown, urgency is 0 (never urgent)
        if item.get('skip_until') and datetime.now() < item['skip_until']:
            return 0.0
        
        if item['last_update'] is None:
            return float('inf')

        delta = datetime.now() - item['last_update']
        
        urgency = delta.total_seconds() / item['polling-interval-in-seconds']
        return urgency

    def calculate_min_sleep_duration(self) -> float:
        """
        Calculate MINIMUM sleep time until ANY item becomes actionable.

        Checks all items and returns the shortest time until any item:
        - Reaches urgency 1.0 (overdue), OR
        - Exits 429 cooldown (skip_until reached)
        
        This ensures we wake up for the SOONEST item, not just the most urgent one.

        Returns:
            Sleep duration in seconds
        """
        min_sleep = float('inf')

        for item in self.live_items:
            # Check if item is in 429 cooldown
            if item.get('skip_until') and datetime.now() < item['skip_until']:
                # Time until cooldown ends
                time_until_cooldown_ends = (item['skip_until'] - datetime.now()).total_seconds()
                min_sleep = min(min_sleep, time_until_cooldown_ends)

            else:
                # Normal urgency calculation
                urgency = self.calculate_urgency(item)
                if urgency < 1.0:  # Only consider items that aren't already overdue
                    # Time until this item becomes urgent (urgency = 1.0)
                    time_until_urgent = (1.0 - urgency) * item['polling-interval-in-seconds']
                    min_sleep = min(min_sleep, time_until_urgent)

        # If all items are overdue, don't sleep
        return min_sleep if min_sleep != float('inf') else 0

    def apply_exponential_backoff(self, item: dict, error_code: int) -> None:
        """
        Apply exponential backoff for rate limit (429), server (5xx), or network errors.
        
        Backoff strategy:
        - 1st error: skip 1 polling interval
        - 2nd consecutive: skip 2 intervals
        - 3rd consecutive: skip 4 intervals
        - Capped at 8x the polling interval
        
        Args:
            item: Item configuration that received the error
            error_code: HTTP status code (429, 5xx) or 0 for network errors
        """
        item['consecutive_backoffs'] = item.get('consecutive_backoffs', 0) + 1
        
        # Skip N polling intervals, where N = 2^(consecutive - 1), capped at 8
        skip_multiplier = min(2 ** (item['consecutive_backoffs'] - 1), 8)
        skip_seconds = item['polling-interval-in-seconds'] * skip_multiplier
        
        item['skip_until'] = datetime.now() + timedelta(seconds=skip_seconds)
        
        if error_code == 429:
            error_type = "rate limited"
        elif error_code == 0:
            error_type = "network error"
        else:
            error_type = f"server error {error_code}"
        
        print(f"  ⏸ {error_type} on {item['market_hash_name']}:{item['apiid']} - "
              f"cooling down {skip_seconds:.0f}s (attempt #{item['consecutive_backoffs']})")

    async def execute_item(self, item: dict) -> None:
        """
        Execute the API call for a specific item.

        Args:
            item: Item configuration to execute
        """
        # Check if item is in cooldown from previous backoff
        if item.get('skip_until') and datetime.now() < item['skip_until']:
            return  # Silently skip, still cooling down
        
        try:
            # match case for MAXIMUM EFFICIENCY
            match item['apiid']:
                
                case 'priceoverview':
                    result = await self.steam_client.fetch_price_overview(
                        appid=item['appid'],
                        market_hash_name=item['market_hash_name'],  # REQUIRED
                        currency=item.get('currency', 1),  # Default to USD
                        country=item.get('country', 'US'),  # Default to US
                        language=item.get('language', 'english')  # Default to english
                    )
                case 'itemordershistogram':
                    result = await self.steam_client.fetch_orders_histogram(
                        appid=item['appid'],
                        item_nameid=item['item_nameid'],  # REQUIRED
                        currency=item.get('currency', 1),  # Default to USD
                        country=item.get('country', 'US'),  # Default to US
                        language=item.get('language', 'english')  # Default to english
                    )
                case 'itemordersactivity':
                    result = await self.steam_client.fetch_orders_activity(
                        item_nameid=item['item_nameid'],  # REQUIRED
                        country=item.get('country', 'US'),  # Default to US
                        language=item.get('language', 'english'),  # Default to english
                        currency=item.get('currency', 1),  # Default to USD
                        two_factor=0
                    )
                    # Activity HTML is already parsed by the client
                    # Success message will be printed after DB storage
                case _:
                    raise ValueError(f"Unknown API endpoint: {item['apiid']}")

            # Store result to database
            await self.data_wizard.store_data(result, item)

            # SUCCESS: Reset backoff tracking
            item['consecutive_backoffs'] = 0
            item['skip_until'] = None
            
            # Update last_update timestamp
            item['last_update'] = datetime.now()

            # Print success message with most relevant data point
            match item['apiid']:
                case 'priceoverview':
                    print(f"  ✓ {item['market_hash_name']}: {result.lowest_price or 'N/A'}")
                case 'itemordershistogram':
                    print(f"  ✓ {item['market_hash_name']}: {result.buy_order_count or 0} orders")
                case 'itemordersactivity':
                    activity_count = len(result.parsed_activities) if result.parsed_activities else 0
                    print(f"  ✓ {item['market_hash_name']}: {activity_count} activities")

        except aiohttp.ClientResponseError as e:
            if e.status == 429 or e.status >= 500:
                # Rate limited or server error - exponential backoff
                self.apply_exponential_backoff(item, e.status)
            elif e.status in (401, 403):
                # Authentication error - likely cookie issue
                print(f"  ✗ HTTP {e.status}: {e.message} - check Steam cookies in .env")
            else:
                # Client error (4xx) - just log, config validated at load time
                print(f"  ✗ HTTP {e.status}: {e.message}")

        except aiohttp.ClientError as e:
            # Network error (timeout, DNS, connection refused) - treat as transient
            print(f"  ⚠ Network error on {item['market_hash_name']}:{item['apiid']} - {e}")
            self.apply_exponential_backoff(item, 0)
            
        except Exception as e:
            # Parse errors, etc. - just log, will retry on next normal cycle
            print(f"  ✗ Error: {e}")

    async def run(self) -> None:
        """
        Main scheduler loop using urgency-based algorithm.

        Algorithm:
        1. Calculate urgency for all items
        2. If max_urgency >= 1.0, execute that item and loop
        3. If max_urgency < 1.0, sleep until next item is overdue
        4. Repeat forever
        """
        async with SteamAPIClient(rate_limiter=self.rate_limiter) as client, SQLinserts() as wizard:
            self.steam_client = client
            self.data_wizard = wizard
            print(f"Live Scheduler started with {len(self.live_items)} items")
            print(f"Database: SQLite at market_data.db")
            if self.rate_limiter is not None:
                print(f"Using shared RateLimiter (orchestrated mode)")

            while True:
                # Execute ALL items that are overdue (urgency >= 1.0)
                executed_any = False
                for item in self.live_items:
                    urgency = self.calculate_urgency(item)
                    if urgency >= 1.0:
                        await self.execute_item(item)
                        executed_any = True

                # If nothing was urgent, sleep until the next item becomes urgent
                if not executed_any:
                    sleep_duration = self.calculate_min_sleep_duration()
                    await asyncio.sleep(sleep_duration)


# Entry point for testing
if __name__ == "__main__":
    scheduler = snoozerScheduler()
    asyncio.run(scheduler.run())
