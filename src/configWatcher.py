"""Mechanism 1: config.yaml file watcher (config -> table).

Watches config.yaml with watchdog. On a real change it pushes the edit into the
tracked_items table via sync_config_to_table. It does NOT touch the scheduler —
the table write fires the Postgres trigger -> NOTIFY, and the scheduler's
listener (Mechanism 3) reacts to that. One emit point, one reaction path.

Loop guard: writeback (table -> config) records the written content's hash in
WRITEBACK_GUARD. When the watcher wakes for that self-caused write, the hash
matches and it skips — so writeback -> file -> watcher -> table never forms an
infinite ping-pong.

watchdog runs the observer on its OWN thread, so the handler hops back onto the
asyncio loop with run_coroutine_threadsafe before doing any async DB work.
"""

import asyncio
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from utility.configTableSync_utility import (
    WRITEBACK_GUARD,
    hash_text,
    sync_config_to_table,
)


class _ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self, dsn: str, config_path: str, loop: asyncio.AbstractEventLoop):
        self.dsn = dsn
        self.config_path = Path(config_path).resolve()
        self.loop = loop
        # Last content we acted on, to drop watchdog's duplicate fire bursts
        # (editors often emit several modified events for one save).
        self._last_seen_hash: str | None = None

    def on_modified(self, event):
        self._maybe_sync(event)

    def on_created(self, event):
        # Atomic-save editors replace the file (delete+create) rather than
        # modify in place, so created must trigger the same path.
        self._maybe_sync(event)

    def _maybe_sync(self, event):
        if event.is_directory:
            return
        if Path(event.src_path).resolve() != self.config_path:
            return

        try:
            content = self.config_path.read_text()
        except FileNotFoundError:
            return  # mid atomic-save; the following create/modify will catch it

        # Loop guard: ignore the echo of our own writeback.
        if WRITEBACK_GUARD.is_own_echo(content):
            print("  ⊘ config change is our own writeback echo — skipping (loop guard)")
            return

        # Drop duplicate fires for the same content.
        h = hash_text(content)
        if h == self._last_seen_hash:
            return
        self._last_seen_hash = h

        print("  ◆ observed config.yaml edit → syncing to tracked_items table")
        # Hop from the watchdog thread back to the event loop to run async DB work.
        future = asyncio.run_coroutine_threadsafe(
            sync_config_to_table(self.dsn, str(self.config_path)), self.loop
        )

        def _report(fut):
            try:
                summary = fut.result()
                print(f"  ✓ config→table sync: {summary} (NOTIFY will drive the scheduler)")
            except Exception as e:
                print(f"  ✗ config→table sync failed: {e}")

        future.add_done_callback(_report)


class ConfigWatcher:
    """Owns the watchdog Observer lifecycle. Start in setup, stop on shutdown."""

    def __init__(self, dsn: str, config_path: str, loop: asyncio.AbstractEventLoop):
        self.config_path = Path(config_path).resolve()
        self._observer = Observer()
        handler = _ConfigChangeHandler(dsn, config_path, loop)
        # Watch the parent directory (watchdog watches dirs, filters to the file)
        # so atomic-save replace (delete+create) is still observed.
        self._observer.schedule(handler, str(self.config_path.parent), recursive=False)

    def start(self) -> None:
        self._observer.start()
        print(f"  ✓ Watching {self.config_path.name} for edits (config→table)")

    def stop(self) -> None:
        self._observer.stop()
        self._observer.join(timeout=5)
