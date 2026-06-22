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
import os
import signal
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from utility.loadConfig_utility import load_config_from_yaml
from utility.loadTrackedItems_utility import fetch_enabled_tracked_items
from utility.configTableSync_utility import (
    CHANNEL,
    install_notify_trigger,
    resolve_item_nameid,
    sync_config_to_table,
)
from src.configWatcher import ConfigWatcher
from src.RateLimiter import RateLimiter
from src.snoozerScheduler import snoozerScheduler
from src.clockworkScheduler import ClockworkScheduler
from utility.feasibility_utility import compute_feasibility

# Streams whose poller can't make a valid call without a resolved item_nameid.
NAMEID_REQUIRED_API_IDS = ('itemordershistogram', 'itemordersactivity')


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
        # Live items sourced from the tracked_items table (the source of truth),
        # loaded once at startup. config.yaml is only the rate-limit + seed source.
        self.tracked_items: list[dict] = []
        self.rate_limiter: Optional[RateLimiter] = None
        self.snoozerScheduler: Optional[snoozerScheduler] = None
        self.clockworkScheduler: Optional[ClockworkScheduler] = None
        self.shutdown_event = asyncio.Event()
        self.dsn: Optional[str] = None
        # Runtime-reactive machinery (config edit / SQL write -> live reconcile).
        self.config_watcher: Optional[ConfigWatcher] = None
        self.listen_conn: Optional[asyncpg.Connection] = None
        # Coalesce a burst of NOTIFYs into one reconcile: the trigger fires per
        # row, so a single config edit can emit several signals — but a reconcile
        # re-reads the whole enabled set, so one pass covers the whole burst.
        self._reconcile_event = asyncio.Event()
        self._pending_notifies = 0
        self._reconcile_worker_task: Optional[asyncio.Task] = None

    def load_config(self):
        """Load config.yaml for the global rate budget (LIMITS) and any
        pricehistory items. The tracked set itself comes from the
        tracked_items table — see load_tracked_items_from_table.
        Validation of the tracked set runs separately in validate_tracked_items.
        """
        print("hello world!")
        self.config = load_config_from_yaml(self.config_path)

        rate_limit = self.config['LIMITS']['REQUESTS']
        window_seconds = self.config['LIMITS']['WINDOW_SECONDS']
        print(f"  I see you have a rate limit: {rate_limit} requests per {window_seconds} seconds")

    def validate_tracked_items(self):
        """Validate the set actually scheduled (self.tracked_items, from the
        table) against the config-level rate budget.

        Only the live set is counted. Feasibility is a SUSTAINED-demand gate:
        does the steady poll load fit the budget so backlog can't grow
        unbounded. pricehistory is hourly archival, not sustained — at hourly
        cadence it contributes window//interval == 0 anyway, and the shared
        rate limiter absorbs its sparse calls gracefully (acquire_token).
        Folding it into the budget would model load that isn't there.

        Invariant this relies on: pricehistory cadence >= window. A sub-window
        pricehistory interval would be real sustained load this gate ignores.

        Per-item polling demand comes from the table; the global budget stays
        config-level (config['LIMITS']) — that's correctly a process-wide knob,
        not per-item. The set passed here is the SAME object setup_schedulers
        hands to the scheduler, so what we validate is what we run.
        """
        rate_limit = self.config['LIMITS']['REQUESTS']
        window_seconds = self.config['LIMITS']['WINDOW_SECONDS']

        # Required fields first (feasibility reads polling-interval-in-seconds)
        self.validate_required_fields(self.tracked_items)
        self.validate_config_feasibility(rate_limit, window_seconds, self.tracked_items)

    def validate_required_fields(self, items: list):
        """
        Validate that each item has all required fields.
        
        Required fields:
        - All items: market_hash_name, api_id, polling-interval-in-seconds, appid
        - histogram/activity: item_nameid (additional)
        
        Args:
            items: List of tracking items to validate
        """
        valid_api_ids = {'priceoverview', 'itemordershistogram', 'itemordersactivity', 'pricehistory'}
        
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
            required = ['market_hash_name', 'api_id', 'polling-interval-in-seconds', 'appid']
            
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
            
            # Validate api_id is recognized
            if item['api_id'] not in valid_api_ids:
                print(f"\n❌ CONFIG ERROR: Item {index + 1} has invalid api_id '{item['api_id']}'")
                print(f"   Valid options: {', '.join(valid_api_ids)}")
                exit(1)
            
            # Check endpoint-specific required fields
            if item['api_id'] in ('itemordershistogram', 'itemordersactivity'):
                if 'item_nameid' not in item:
                    print(f"\n❌ CONFIG ERROR: Item {index + 1} missing 'item_nameid' (required for {item['api_id']})")
                    print(f"   Item: {item.get('market_hash_name', 'unknown')}")
                    exit(1)

    def validate_config_feasibility(self, rate_limit: int, window_seconds: int, items: list):
        """
        Validate that config is feasible given rate limits. BOOT path — exits the
        process on an infeasible set, because there's no prior good state to keep.

        The runtime listener uses the same compute_feasibility() math but only
        logs and rejects the change (keeps current pollers); see
        handle_tracked_items_changed.

        Args:
            rate_limit: Max requests per window
            window_seconds: Time window in seconds
            items: List of tracking items with their configs
        """
        intervals = [item['polling-interval-in-seconds'] for item in items]
        ok, total_reqs, utilization = compute_feasibility(rate_limit, window_seconds, intervals)

        if not ok:
            print(f"\n❌ CONFIG ERROR: Infeasible configuration")
            print(f"   Calculated: {total_reqs} requests per {window_seconds}s")
            print(f"   Limit: {rate_limit} requests per {window_seconds}s")
            print(f"   Adjust polling intervals or reduce tracked items")
            exit(1)

        # Success - config is feasible
        print(f"  ✓ Config feasible: {total_reqs} req/{window_seconds}s ({utilization:.1f}% capacity)")

        # Warn about startup burst
        if len(items) > rate_limit:
            print(f"  ⚠ Startup: {len(items)} items will fire initially (rate limiter will queue them)")

    async def load_tracked_items_from_table(self):
        """Load enabled live items from tracked_items (the source of truth).

        Startup-read only. Stored on self.tracked_items for setup_schedulers.
        """
        dsn = os.getenv("CS2_PG_DSN")
        if not dsn:
            print("\n❌ CS2_PG_DSN is not set. Required to read tracked_items.")
            exit(1)
        self.dsn = dsn
        self.tracked_items = await fetch_enabled_tracked_items(dsn)

    def setup_schedulers(self):
        """Create scheduler instances with shared rate limiter."""
        print("\n")

        # Create single shared rate limiter (CRITICAL for API compliance)
        rate_limit = self.config['LIMITS']['REQUESTS']
        window_seconds = self.config['LIMITS']['WINDOW_SECONDS']
        self.rate_limiter = RateLimiter(max_requests=rate_limit, window_seconds=window_seconds)
        print(f"  ✓ Shared RateLimiter created ({rate_limit} req/{window_seconds}s)")

        # Backend: Postgres/Timescale only. DSN comes from CS2_PG_DSN (.env),
        # never hardcoded. There is no SQLite fallback — fail loudly if unset.
        timescale_dsn = os.getenv("CS2_PG_DSN")
        if not timescale_dsn:
            print("\n❌ CS2_PG_DSN is not set. Postgres is required (no SQLite fallback).")
            print("   Set it in .env, e.g. CS2_PG_DSN=postgresql://user:pass@localhost:5432/cs2market")
            exit(1)
        print("  ✓ Database: Postgres/Timescale (CS2_PG_DSN)")
        self.timescale_dsn = timescale_dsn

        # Live items now come from the tracked_items table (loaded at startup in
        # run()), NOT config.yaml. The table is the single source of truth for
        # what's tracked and how. Startup-read only — no runtime hot-reload this
        # pass. History (pricehistory) is a separate bulk archival job and is
        # intentionally not part of tracked_items.
        live_items = self.tracked_items
        history_items = [
            item for item in self.config['TRACKING_ITEMS']
            if item['api_id'] == 'pricehistory'
        ]
        print(f"  ✓ Sourced {len(live_items)} live item(s) from tracked_items table")

        # Always create the live scheduler, even with an empty set: it must exist
        # so a runtime-added item (config edit or SQL write -> NOTIFY -> reconcile)
        # has a live poller set to grow into without a restart. Empty-set loop
        # idles instead of busy-spinning (see calculate_min_sleep_duration).
        self.snoozerScheduler = snoozerScheduler(
            live_items=live_items,
            rate_limiter=self.rate_limiter,
            timescale_dsn=self.timescale_dsn
        )
        print(f"  ✓ Started HIGH frequency tracking on ({len(live_items)} items)")

        if history_items:
            self.clockworkScheduler = ClockworkScheduler(
                items=history_items,
                rate_limiter=self.rate_limiter,
                timescale_dsn=self.timescale_dsn
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

    async def start_change_listener(self):
        """Mechanism 3: LISTEN on the tracked_items_changed channel.

        Opens a dedicated long-lived connection (asyncpg LISTEN can't share the
        ingestion pool). Every table write — file-watcher, frontend endpoint, or
        manual SQL — funnels through the DB trigger to this one handler.

        The asyncpg callback is synchronous and runs on the loop; it can't await,
        so it just flags the coalescing worker. The worker debounces a burst of
        NOTIFYs (the per-row trigger fires several times for one logical edit)
        into a single reconcile.
        """
        self.listen_conn = await asyncpg.connect(self.dsn)

        # The asyncpg callback is sync and must not block; it just flags the
        # coalescing worker, which does the actual (debounced) reconcile.
        def _on_notify(conn, pid, channel, payload):
            self._pending_notifies += 1
            self._reconcile_event.set()

        await self.listen_conn.add_listener(CHANNEL, _on_notify)
        self._reconcile_worker_task = asyncio.create_task(
            self._reconcile_worker(), name="reconcile-worker"
        )
        print(f"  ✓ Listening on '{CHANNEL}' — table changes reconcile live")

    async def _reconcile_worker(self):
        """Drain coalesced NOTIFYs into single reconciles.

        Waits for at least one signal, then a short quiet window so a burst (the
        per-row trigger fires several times for one logical edit) collapses into
        one reconcile instead of N redundant +0/-0 passes.
        """
        while not self.shutdown_event.is_set():
            await self._reconcile_event.wait()
            # Quiet window: let the rest of the burst land before reconciling.
            await asyncio.sleep(0.25)
            self._reconcile_event.clear()
            absorbed = self._pending_notifies
            self._pending_notifies = 0
            try:
                await self.handle_tracked_items_changed(absorbed)
            except Exception as e:
                print(f"  ✗ reconcile failed: {e}")

    async def handle_tracked_items_changed(self, absorbed_notifies: int):
        """React to tracked_items change(s): re-read, gate on feasibility, reconcile.

        Steps:
          1. Re-read the enabled set from the table (the runtime master).
          2. Resolve item_nameid for any histogram/activity row missing it
             (direct SQL inserts may omit it). Unresolvable -> drop that one
             item with a clear reason; the rest still apply.
          3. Feasibility gate against the config budget. Infeasible -> reject the
             WHOLE change, log, keep current pollers untouched. Never apply a set
             that would blow the rate limit.
          4. Feasible -> reconcile the live poller set (no restart).
        """
        new_items = await fetch_enabled_tracked_items(self.dsn)

        # --- nameid resolution (reject items that can't poll) ---
        usable = []
        for item in new_items:
            if item['api_id'] in NAMEID_REQUIRED_API_IDS and not item.get('item_nameid'):
                nameid = resolve_item_nameid(item['market_hash_name'])
                if nameid is None:
                    print(f"\n🔔 tracked_items changed ({absorbed_notifies} signal(s)) — reconciling")
                    print(f"  ✗ REJECTED {item['market_hash_name']}:{item['api_id']} — "
                          f"no item_nameid resolvable (required for this stream)")
                    continue
                item['item_nameid'] = nameid
            usable.append(item)

        # --- feasibility gate (mandatory before applying) ---
        rate_limit = self.config['LIMITS']['REQUESTS']
        window_seconds = self.config['LIMITS']['WINDOW_SECONDS']
        intervals = [item['polling-interval-in-seconds'] for item in usable]
        ok, total_reqs, utilization = compute_feasibility(rate_limit, window_seconds, intervals)
        if not ok:
            print(f"\n🔔 tracked_items changed ({absorbed_notifies} signal(s)) — reconciling")
            print(f"  ✗ REJECTED change — infeasible: {total_reqs} req/{window_seconds}s "
                  f"exceeds budget {rate_limit}. Keeping current pollers unchanged.")
            return

        # --- apply: reconcile the live poller set, no restart ---
        summary = self.snoozerScheduler.reconcile_live_set(usable)
        # Stay quiet on a pure no-op (e.g. a coalesced burst that netted no
        # structural change); only announce real add/remove churn.
        if summary['added'] or summary['removed']:
            print(f"\n🔔 tracked_items changed ({absorbed_notifies} signal(s) coalesced) — reconciled "
                  f"({total_reqs} req/{window_seconds}s, {utilization:.1f}% capacity): "
                  f"+{len(summary['added'])} added, -{len(summary['removed'])} removed, "
                  f"{summary['total']} live")
            if summary['added']:
                print(f"      added:   {summary['added']}")
            if summary['removed']:
                print(f"      removed: {summary['removed']}")

    async def run(self):
        """
        Main orchestrator loop.

        Runs all schedulers concurrently until shutdown signal or config change.
        """
        # Load config.yaml for the global rate budget (LIMITS) + pricehistory items
        self.load_config()

        # DUAL-MASTER BOOT RULE: config -> table FIRST, then read the table.
        # Because every table change writes back to config (Mechanism 4, in the
        # writer), config already reflects frontend/SQL-added items at boot — so
        # seeding the table from config never clobbers them. The table is the
        # runtime master; config is the boot input + human-editable mirror.
        dsn = os.getenv("CS2_PG_DSN")
        if not dsn:
            print("\n❌ CS2_PG_DSN is not set. Required for the tracked_items pipeline.")
            exit(1)
        self.dsn = dsn

        # Single emit point: install the NOTIFY trigger before any table write.
        await install_notify_trigger(dsn)
        # config -> table on boot (seed/upsert + disable rows config dropped).
        boot_sync = await sync_config_to_table(dsn, self.config_path)
        print(f"  ✓ Boot config→table sync: {boot_sync}")

        # Load the tracked set from the tracked_items table (source of truth)
        await self.load_tracked_items_from_table()

        # Validate the table-loaded set (the one actually scheduled) against the
        # config rate budget — same object that setup_schedulers runs.
        self.validate_tracked_items()

        # Setup schedulers with shared rate limiter
        self.setup_schedulers()

        # Setup signal handlers for graceful shutdown
        self.setup_signal_handlers()

        # Reactive machinery: LISTEN for table changes, then watch config.yaml.
        # Listener up before watcher so the very first watcher-driven table write
        # is already being heard.
        await self.start_change_listener()
        self.config_watcher = ConfigWatcher(dsn, self.config_path, asyncio.get_event_loop())
        self.config_watcher.start()

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
        finally:
            # Tear down the reactive machinery (worker + watcher thread + conn).
            if self._reconcile_worker_task:
                self._reconcile_worker_task.cancel()
                try:
                    await self._reconcile_worker_task
                except asyncio.CancelledError:
                    pass
            if self.config_watcher:
                self.config_watcher.stop()
            if self.listen_conn:
                # Closing the connection drops its listeners.
                await self.listen_conn.close()

        print("\n✓ All schedulers stopped gracefully")


async def main():
    """Entry point for the backend."""
    load_dotenv()  # Pull CS2_PG_DSN (and Steam cookies) from .env
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
