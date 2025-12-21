"""
Live Data Scheduler - Urgency-based scheduling for real-time market data.

This scheduler manages priceoverview, itemordershistogram, and itemordersactivity
endpoints based on their latency requirements using an urgency scoring system.
"""

import asyncio
from datetime import datetime
from typing import List, Optional
from steamAPIclient import SteamAPIClient
from loadConfig import load_config_from_yaml


class LiveScheduler:
    """
    Schedules live API calls based on urgency (how overdue each item is).

    Items with urgency >= 1.0 are overdue and need immediate execution.
    The item with highest urgency is always executed first.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the live scheduler.

        Args:
            config_path: Path to the YAML configuration file
        """
        self.config = load_config_from_yaml(config_path)
        self.live_items = self._load_live_items()
        self.steam_client: Optional[SteamAPIClient] = None  # Will be initialized in run()

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
        print(f"[{datetime.now()}] Executing {item['apiid']} for {item['market_hash_name']}")

        try:
            # match case for MAXIMUM EFFICIENCY
            match item['apiid']:
                
                case 'priceoverview':
                    result = await self.steam_client.fetch_price_overview(
                        appid=item['appid'],
                        market_hash_name=item['market_hash_name']
                    )
                case 'itemordershistogram':
                    result = await self.steam_client.fetch_orders_histogram(
                        appid=item['appid'],
                        item_nameid=item.get('item_nameid', 0),
                        currency=item.get('currency', 1),
                        country=item.get('country', 'US'),
                        language=item.get('language', 'english')
                    )
                case 'itemordersactivity':
                    result = await self.steam_client.fetch_orders_activity(
                        item_nameid=item.get('item_nameid', 0),
                        country=item.get('country', 'US'),
                        language=item.get('language', 'english'),
                        currency=item.get('currency', 1),
                        two_factor=0
                    )
                    # Activity HTML is already parsed by the client
                    if result.parsed_activities:
                        print(f"  ✓ Success: Parsed {len(result.parsed_activities)} activity entries")
                    else:
                        print(f"  ✓ Success: No activity (empty list is normal)")
                case _:
                    raise ValueError(f"Unknown API endpoint: {item['apiid']}")
            
            # Update last_update timestamp
            item['last_update'] = datetime.now()

            # TODO: Store result to database/file

            if item['apiid'] != 'itemordersactivity':
                print(f"  ✓ Success: {result.success}")

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
        async with SteamAPIClient() as client:
            self.steam_client = client
            print(f"Live Scheduler started with {len(self.live_items)} items")

            while True:
                mostUrgentItemTuple = self.find_most_urgent_item()
                if mostUrgentItemTuple[1] >= 1.0:
                    await self.execute_item(mostUrgentItemTuple[0])
                else:
                    sleep_duration = self.calculate_sleep_duration(mostUrgentItemTuple[1], mostUrgentItemTuple[0])
                    await asyncio.sleep(sleep_duration)


# Entry point for testing
if __name__ == "__main__":
    scheduler = LiveScheduler()
    asyncio.run(scheduler.run())
