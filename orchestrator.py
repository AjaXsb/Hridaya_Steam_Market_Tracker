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
from loadConfig import load_config_from_yaml
from RateLimiter import RateLimiter
from snoozerScheduler import snoozerScheduler
from clockworkScheduler import ClockworkScheduler


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

        print(f"  I see you want to make: {rate_limit} requests per {window_seconds} seconds")
        

        # TODO: User will implement feasibility math here
        # Check if the combination of schedulers + items can stay within rate limits
        # Placeholder for now:
        self._validate_config_feasibility(rate_limit, window_seconds, tracking_items)

    def _validate_config_feasibility(self, rate_limit: int, window_seconds: int, items: list):
        """
        Validate that config is feasible given rate limits.

        TODO: User will write the math here to check:
        - Can snoozer + clockwork schedulers both run without exceeding rate_limit?
        - Are item latencies realistic given rate constraints?
        - Should we warn or error if infeasible?

        Args:
            rate_limit: Max requests per window
            window_seconds: Time window in seconds
            items: List of tracking items with their configs
        """
        # Placeholder - user will implement
        print("  Config feasibility check: TODO (user will implement)")
        pass

    def setup_schedulers(self):
        """Create scheduler instances with shared rate limiter."""
        print("\n")

        # Create single shared rate limiter (CRITICAL for API compliance)
        self.rate_limiter = RateLimiter()
        print("  ✓ Timekeeper has joined the server")

        # Filter items by type: live items (not pricehistory) vs history items
        live_items = [
            item for item in self.config['TRACKING_ITEMS']
            if item['apiid'] != 'pricehistory'
        ]

        history_items = [
            item for item in self.config['TRACKING_ITEMS']
            if item['apiid'] == 'pricehistory'
        ]

        # Create schedulers with shared rate limiter
        if live_items:
            self.snoozerScheduler = snoozerScheduler(
                live_items=live_items,
                rate_limiter=self.rate_limiter
            )
            print(f"  ✓ Started HIGH frequency tracking on ({len(live_items)} items)")

        if history_items:
            self.clockwork_scheduler = ClockworkScheduler(
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
            shutdown_task = asyncio.create_task(self.shutdown_event.wait())
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
                if task.get_name() != "Task-shutdown" and not task.cancelled():
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
