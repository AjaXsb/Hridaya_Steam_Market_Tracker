"""
Orchestrator - Backend entry point for CS2 Market Tracker.

Responsibilities:
- Load configuration from config.yaml
- Validate config feasibility (rate limits vs tracking items)
- Create shared RateLimiter instance (critical for API compliance)
- Initialize and coordinate all schedulers
- Handle graceful shutdown on SIGINT/SIGTERM
"""

import asyncio
import signal
from typing import Optional
from utility.loadConfig_utility import load_config_from_yaml
from src.RateLimiter import RateLimiter
from src.snoozerScheduler import snoozerScheduler
from src.clockworkScheduler import ClockworkScheduler


class Orchestrator:
    """
    Coordinates all schedulers with shared rate limiting.

    Think of it as a choir conductor - ensures all parts (schedulers)
    work in harmony without exceeding Steam's API rate limits.
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize orchestrator with configuration.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config: Optional[dict] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.snoozerScheduler: Optional[snoozerScheduler] = None
        self.clockworkScheduler: Optional[ClockworkScheduler] = None
        self.shutdown_event = asyncio.Event()

    def load_and_validate_config(self):
        """Load config and validate feasibility."""
        print("hello world!")
        self.config = load_config_from_yaml(self.config_path)

        # Extract config values
        rate_limit = self.config['LIMITS']['REQUESTS']
        window_seconds = self.config['LIMITS']['WINDOW_SECONDS']
        tracking_items = self.config['TRACKING_ITEMS']

        print(f"  I see you have a rate limit: {rate_limit} requests per {window_seconds} seconds")

        # Validate required fields exist before checking feasibility
        self._validate_required_fields(tracking_items)

        self._validate_config_feasibility(rate_limit, window_seconds, tracking_items)

    def _validate_required_fields(self, items: list):
        """
        Validate that each item has all required fields.
        
        Required fields:
        - All items: market_hash_name, apiid, polling-interval-in-seconds, appid
        - histogram/activity: item_nameid (additional)
        
        Args:
            items: List of tracking items to validate
        """
        valid_apiids = {'priceoverview', 'itemordershistogram', 'itemordersactivity', 'pricehistory'}
        
        # Popular Steam app IDs for helpful error messages
        popular_appids = {
            730: "Counter-Strike 2 (CS2)",
            570: "Dota 2",
            440: "Team Fortress 2",
            252490: "Rust",
            753: "Steam (trading cards, backgrounds, emoticons)"
        }
        
        for index, item in enumerate(items):
            # Check universal required fields
            required = ['market_hash_name', 'apiid', 'polling-interval-in-seconds', 'appid']
            
            for field in required:
                if field not in item:
                    print(f"\n❌ CONFIG ERROR: Item {index + 1} missing required field '{field}'")
                    print(f"   Item: {item}")
                    
                    # Helpful hint for appid
                    if field == 'appid':
                        print(f"\n   Popular App IDs:")
                        for appid, name in popular_appids.items():
                            print(f"     {appid}: {name}")
                    
                    exit(1)
            
            # Validate apiid is recognized
            if item['apiid'] not in valid_apiids:
                print(f"\n❌ CONFIG ERROR: Item {index + 1} has invalid apiid '{item['apiid']}'")
                print(f"   Valid options: {', '.join(valid_apiids)}")
                exit(1)
            
            # Check endpoint-specific required fields
            if item['apiid'] in ('itemordershistogram', 'itemordersactivity'):
                if 'item_nameid' not in item:
                    print(f"\n❌ CONFIG ERROR: Item {index + 1} missing 'item_nameid' (required for {item['apiid']})")
                    print(f"   Item: {item.get('market_hash_name', 'unknown')}")
                    exit(1)

    def _validate_config_feasibility(self, rate_limit: int, window_seconds: int, items: list):
        """
        Validate that config is feasible given rate limits.

        Calculates maximum requests per window assuming worst-case (all items synchronized).
        Real usage will typically be lower due to urgency-based scheduling spreading requests.

        Args:
            rate_limit: Max requests per window
            window_seconds: Time window in seconds
            items: List of tracking items with their configs
        """
        total_reqs = 0

        for item in items:
            reqs_per_window = window_seconds // item['polling-interval-in-seconds']
            total_reqs += reqs_per_window

        if total_reqs > rate_limit:
            print(f"\n❌ CONFIG ERROR: Infeasible configuration")
            print(f"   Calculated: {total_reqs} requests per {window_seconds}s")
            print(f"   Limit: {rate_limit} requests per {window_seconds}s")
            print(f"   Adjust polling intervals or reduce tracked items")
            exit(1)

        # Success - config is feasible
        utilization = (total_reqs / rate_limit) * 100
        print(f"  ✓ Config feasible: {total_reqs} req/{window_seconds}s ({utilization:.1f}% capacity)")

        # Warn about startup burst
        if len(items) > rate_limit:
            print(f"  ⚠ Startup: {len(items)} items will fire initially (rate limiter will queue them)")

    def setup_schedulers(self):
        """Create scheduler instances with shared rate limiter."""
        print("\n")

        # Create single shared rate limiter (CRITICAL for API compliance)
        self.rate_limiter = RateLimiter()
        print("  ✓ Shared RateLimiter created")
        print("  ✓ Database: SQLite at market_data.db")

        # Filter items by type: live items (not pricehistory) vs history items
        live_items = []
        history_items = []

        for item in self.config['TRACKING_ITEMS']:
            if item['apiid'] == 'pricehistory':
                history_items.append(item)
            else:
                live_items.append(item)

        # Create schedulers with shared rate limiter
        if live_items:
            self.snoozerScheduler = snoozerScheduler(
                live_items=live_items,
                rate_limiter=self.rate_limiter
            )
            print(f"  ✓ Started HIGH frequency tracking on ({len(live_items)} items)")

        if history_items:
            self.clockworkScheduler = ClockworkScheduler(
                items=history_items,
                rate_limiter=self.rate_limiter
            )
            print(f"  ✓ Started ARCHIVAL work + all known historical snapshots available right now on ({len(history_items)} items)")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        loop = asyncio.get_event_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self.shutdown())
            )

    async def shutdown(self):
        """Handle graceful shutdown."""
        print("\n\nShutdown signal received. Stopping schedulers...")
        self.shutdown_event.set()

    async def run(self):
        """
        Main orchestrator loop.

        Runs all schedulers concurrently until shutdown signal or config change.
        """
        # Load and validate configuration
        self.load_and_validate_config()

        # Setup schedulers with shared rate limiter
        self.setup_schedulers()

        # Setup signal handlers for graceful shutdown
        self.setup_signal_handlers()

        print("\n")
        print("GO TIME!")
        print("\n")
        print(f"Press Ctrl+C to stop")
        print("="*60 + "\n")

        # Create tasks for all schedulers
        tasks = []

        if self.snoozerScheduler:
            tasks.append(asyncio.create_task(
                self.snoozerScheduler.run(),
                name="live"
            ))

        if self.clockworkScheduler:
            tasks.append(asyncio.create_task(
                self.clockworkScheduler.run(),
                name="clockwork"
            ))

        if not tasks:
            print("Warning: No schedulers configured. Exiting.")
            return

        # Run all schedulers concurrently until shutdown
        try:
            # Wait for shutdown event or any task to fail
            shutdown_task = asyncio.create_task(self.shutdown_event.wait(), name="shutdown")
            done, pending = await asyncio.wait(
                tasks + [shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Check if any scheduler task failed
            for task in done:
                if task.get_name() != "shutdown" and not task.cancelled():
                    exc = task.exception()
                    if exc:
                        print(f"Scheduler {task.get_name()} failed with error: {exc}")
                        raise exc

        except asyncio.CancelledError:
            print("Orchestrator cancelled")

        print("\n✓ All schedulers stopped gracefully")


async def main():
    """Entry point for the backend."""
    orchestrator = Orchestrator(config_path="config.yaml")
    await orchestrator.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Exiting...")
    except Exception as e:
        print(f"\nFatal error: {e}")
        raise
