"""
Live Data Scheduler - Urgency-based scheduling for real-time market data.

This scheduler manages priceoverview, itemordershistogram, and itemordersactivity
endpoints based on their latency requirements using an urgency scoring system.
"""

import asyncio
from datetime import datetime
from typing import List, Optional
from steamAPIclient import SteamAPIClient
from RateLimiter import RateLimiter
from loadConfig import load_config_from_yaml
from SQLinserts import SQLinserts


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
            self.live_items = self._load_live_items()

        self.steam_client: Optional[SteamAPIClient] = None  # Will be initialized in run()
        self.data_wizard: Optional[SQLinserts] = None  # Will be initialized in run()

    def _load_live_items(self) -> List[dict]:
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

        Args:
            item: Item configuration with last_update and polling-interval-in-seconds

        Returns:
            Urgency score (>= 1.0 means overdue, < 1.0 means current)
        """
        if item['last_update'] == None:
            return float('inf')

        delta = datetime.now() - item['last_update']
        
        urgency = delta.total_seconds() / item['polling-interval-in-seconds']
        print(urgency)
        return urgency

    def find_most_urgent_item(self) -> tuple[dict, float]:
        """
        Find the item with the highest urgency score.

        Returns:
            Tuple of (most_urgent_item, urgency_score)
        """
        urgencies = []
        for item in self.live_items:
            urgency = self.calculate_urgency(item)
            urgencies.append((item, urgency))

        # Find tuple with max urgency (second element)
        mostUrgentItemTuple = max(urgencies, key=lambda x: x[1])

        return mostUrgentItemTuple


    def calculate_sleep_duration(self, max_urgency: float, max_urgency_item: dict) -> float:
        """
        Calculate how long to sleep until the next item becomes overdue.

        Args:
            max_urgency: The highest urgency score among all items
            max_urgency_item: The item with the highest urgency

        Returns:
            Sleep duration in seconds
        """
        # Time until urgency reaches 1.0 = (1.0 - current_urgency) * polling rate
        sleepTime = (1.0 - max_urgency) * max_urgency_item['polling-interval-in-seconds']
        return sleepTime


    async def execute_item(self, item: dict) -> None:
        """
        Execute the API call for a specific item.

        Args:
            item: Item configuration to execute
        """
        try:
            # match case for MAXIMUM EFFICIENCY
            match item['apiid']:
                
                case 'priceoverview':
                    result = await self.steam_client.fetch_price_overview(
                        appid=item.get('appid', 730),  # Default to CS2
                        market_hash_name=item['market_hash_name'],  # REQUIRED
                        currency=item.get('currency', 1)  # Default to USD
                    )
                case 'itemordershistogram':
                    result = await self.steam_client.fetch_orders_histogram(
                        appid=item.get('appid', 730),  # Default to CS2
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

            # Update last_update timestamp
            item['last_update'] = datetime.now()

            # Print success message with most relevant data point
            match item['apiid']:
                case 'priceoverview':
                    print(f"  ✓ {item['market_hash_name']}: {result.lowest_price or 'N/A'}")
                case 'itemordershistogram':
                    print(f"  ✓ {item['market_hash_name']}: {result.buy_order_count or 0} orders")
                case 'itemordersactivity':
                    from dataClasses import OrdersActivityData
                    if isinstance(result, OrdersActivityData):
                        activity_count = len(result.parsed_activities) if result.parsed_activities else 0
                        print(f"  ✓ {item['market_hash_name']}: {activity_count} activities")

                        # DEBUG: Dump activities for sanity check
                        if result.parsed_activities:
                            print(f"  📋 Activity Details:")
                            for i, activity in enumerate(result.parsed_activities[:5], 1):  # Show first 5
                                print(f"      {i}. {activity.action} - {activity.price} {activity.currency} @ {activity.timestamp}")
                            if len(result.parsed_activities) > 5:
                                print(f"      ... and {len(result.parsed_activities) - 5} more")

        except Exception as e:
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
                    mostUrgentItemTuple = self.find_most_urgent_item()
                    sleep_duration = self.calculate_sleep_duration(mostUrgentItemTuple[1], mostUrgentItemTuple[0])
                    await asyncio.sleep(sleep_duration)


# Entry point for testing
if __name__ == "__main__":
    scheduler = snoozerScheduler()
    asyncio.run(scheduler.run())
