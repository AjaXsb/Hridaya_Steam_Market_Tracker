"""Bidirectional config.yaml <-> tracked_items sync + the NOTIFY plumbing.

Dual-master rule (stated once, here, and mirrored in cerebro):
  * config -> table on BOOT (seed/upsert the table from the human-editable file)
  * table -> config on EVERY table write that should mirror back (the writer
    calls regenerate_config_from_table after a successful write)
  * the TABLE is the runtime master; config.yaml is the boot input + the
    human-editable mirror.

This module owns three directions plus the loop-guard:
  * sync_config_to_table        config -> table   (file watcher / boot)
  * regenerate_config_from_table table -> config  (table writers, NOT NOTIFY)
  * install_notify_trigger       table -> NOTIFY  (single emit point)
  * WRITEBACK_GUARD              breaks the file<->table ping-pong

The watcher (file->table) deliberately does NOT write back to config — its
source IS the file. Only table writers (frontend endpoint, later) call
regenerate_config_from_table. That asymmetry is what keeps the two inverse
operations from looping; the hash guard below closes the remaining one-write
echo when a writeback does touch the file.
"""

import hashlib
import io
from pathlib import Path
from typing import Optional

import asyncpg
from ruamel.yaml import YAML

from utility.loadConfig_utility import load_config_from_yaml, fetch_cs2_item_name_ids

# Round-trip YAML for writeback: preserves the human-maintained comments,
# key order, and quoting in config.yaml instead of flattening them away.
_yaml_rt = YAML()
_yaml_rt.preserve_quotes = True

# config api_id <-> tracked_items.stream. pricehistory is a bulk archival job,
# not a live stream, so it is intentionally excluded from tracked_items (matches
# seed_tracked_items.API_ID_TO_STREAM and loadTrackedItems STREAM_TO_API_ID).
API_ID_TO_STREAM = {
    "priceoverview": "priceoverview",
    "itemordershistogram": "histogram",
    "itemordersactivity": "activity",
}
STREAM_TO_API_ID = {v: k for k, v in API_ID_TO_STREAM.items()}

CHANNEL = "tracked_items_changed"


class _WritebackGuard:
    """Records the hash of the file content the writeback path last wrote, so
    the watcher can recognise and ignore its own echo.

    One process, one event loop -> a plain attribute is enough; no lock needed.
    """

    def __init__(self):
        self.last_written_hash: Optional[str] = None

    def remember(self, content: str) -> None:
        self.last_written_hash = hash_text(content)

    def is_own_echo(self, content: str) -> bool:
        return self.last_written_hash is not None and hash_text(content) == self.last_written_hash


WRITEBACK_GUARD = _WritebackGuard()


def hash_text(text: str) -> str:
    """Stable content hash used by the loop-guard."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def resolve_item_nameid(market_hash_name: str) -> Optional[int]:
    """Look up the numeric item_nameid for a market_hash_name.

    histogram/activity streams cannot make a valid API call without this id, so
    the reconcile/sync paths must fill it before starting a poller. Returns None
    when the name isn't in the id map (caller rejects that item with a reason).
    """
    return fetch_cs2_item_name_ids().get(market_hash_name)


def build_desired_rows_from_config(config: dict) -> list[dict]:
    """Shape config TRACKING_ITEMS into tracked_items rows (live streams only).

    load_config_from_yaml already resolved/discarded missing nameids, so every
    row returned here is safe to start a poller from.
    """
    rows = []
    for item in config.get("TRACKING_ITEMS", []):
        stream = API_ID_TO_STREAM.get(item["api_id"])
        if stream is None:
            continue  # pricehistory / non-live — not part of the tracked set
        rows.append(
            {
                "market_hash_name": item["market_hash_name"],
                "appid": item["appid"],
                "item_nameid": item.get("item_nameid"),
                "stream": stream,
                "currency": item.get("currency", 1),
                "country": item.get("country", "US"),
                "language": item.get("language", "english"),
                "poll_interval_sec": item["polling-interval-in-seconds"],
            }
        )
    return rows


async def install_notify_trigger(dsn: str) -> None:
    """Create the AFTER INSERT/UPDATE/DELETE trigger that pg_notifies on ANY
    tracked_items write.

    The DB is the single emit point: file-watcher, frontend endpoint, and manual
    SQL all funnel through the same trigger, so every writer produces the same
    signal and the scheduler listener doesn't care who wrote.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION notify_tracked_items_changed()
            RETURNS trigger AS $$
            DECLARE
                row_data RECORD;
                payload TEXT;
            BEGIN
                IF (TG_OP = 'DELETE') THEN
                    row_data := OLD;
                ELSE
                    row_data := NEW;
                END IF;
                payload := json_build_object(
                    'op', TG_OP,
                    'market_hash_name', row_data.market_hash_name,
                    'stream', row_data.stream
                )::text;
                PERFORM pg_notify('{CHANNEL}', payload);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;

            DROP TRIGGER IF EXISTS tracked_items_changed_trg ON tracked_items;
            CREATE TRIGGER tracked_items_changed_trg
                AFTER INSERT OR UPDATE OR DELETE ON tracked_items
                FOR EACH ROW EXECUTE FUNCTION notify_tracked_items_changed();
            """
        )
    finally:
        await conn.close()


async def sync_config_to_table(dsn: str, config_path: str = "config.yaml") -> dict:
    """config -> table. Upsert every live item in config, disable any enabled
    row no longer present in config.

    Used on boot (seed/refresh the table) and by the file watcher (apply a human
    edit). Idempotent: ON CONFLICT DO UPDATE means re-running with unchanged
    config is a no-op write-wise. Each write still fires the trigger -> NOTIFY,
    which is how the running scheduler learns about the change.

    Does NOT write back to config — the file is this path's source of truth.
    Returns a small summary for logging.
    """
    config = load_config_from_yaml(config_path)
    desired = build_desired_rows_from_config(config)
    desired_keys = {(r["market_hash_name"], r["stream"]) for r in desired}

    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            for r in desired:
                # LOAD-BEARING — do not drop this WHERE. It suppresses no-op
                # writes: an unchanged row isn't rewritten, so the AFTER trigger
                # doesn't fire and no spurious NOTIFY is emitted. Two things rely
                # on it:
                #   1. Without it, ON CONFLICT DO UPDATE rewrites every row on
                #      every sync, so one config edit storms the listener with a
                #      NOTIFY per tracked row.
                #   2. It is the CROSS-PROCESS loop guard. The WRITEBACK_GUARD
                #      hash only suppresses the watcher's echo within ONE process.
                #      When the API process writes config (writeback), the
                #      scheduler process's watcher still sees the file change and
                #      re-syncs config->table here — this WHERE makes that re-sync
                #      a no-op (no write, no NOTIFY), which is what stops the
                #      file<->table ping-pong from looping across processes.
                await conn.execute(
                    """
                    INSERT INTO tracked_items
                        (market_hash_name, appid, item_nameid, stream,
                         currency, country, language, poll_interval_sec, enabled)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
                    ON CONFLICT (market_hash_name, stream) DO UPDATE SET
                        appid = EXCLUDED.appid,
                        item_nameid = EXCLUDED.item_nameid,
                        currency = EXCLUDED.currency,
                        country = EXCLUDED.country,
                        language = EXCLUDED.language,
                        poll_interval_sec = EXCLUDED.poll_interval_sec,
                        enabled = TRUE
                    WHERE tracked_items.appid             IS DISTINCT FROM EXCLUDED.appid
                       OR tracked_items.item_nameid       IS DISTINCT FROM EXCLUDED.item_nameid
                       OR tracked_items.currency          IS DISTINCT FROM EXCLUDED.currency
                       OR tracked_items.country           IS DISTINCT FROM EXCLUDED.country
                       OR tracked_items.language          IS DISTINCT FROM EXCLUDED.language
                       OR tracked_items.poll_interval_sec IS DISTINCT FROM EXCLUDED.poll_interval_sec
                       OR tracked_items.enabled           IS DISTINCT FROM TRUE
                    """,
                    r["market_hash_name"], r["appid"], r["item_nameid"], r["stream"],
                    r["currency"], r["country"], r["language"], r["poll_interval_sec"],
                )

            # Disable rows that config no longer lists (config is master on this
            # path). Disable, not delete — keeps the row for re-enable + history.
            enabled_rows = await conn.fetch(
                "SELECT market_hash_name, stream FROM tracked_items WHERE enabled = TRUE"
            )
            disabled = 0
            for row in enabled_rows:
                if (row["market_hash_name"], row["stream"]) not in desired_keys:
                    await conn.execute(
                        "UPDATE tracked_items SET enabled = FALSE "
                        "WHERE market_hash_name = $1 AND stream = $2",
                        row["market_hash_name"], row["stream"],
                    )
                    disabled += 1
    finally:
        await conn.close()

    return {"upserted": len(desired), "disabled": disabled}


async def regenerate_config_from_table(dsn: str, config_path: str = "config.yaml") -> None:
    """table -> config. Rewrite TRACKING_ITEMS from the enabled rows while
    preserving everything else in the file via a ruamel round-trip.

    Round-trip (not safe_dump): config.yaml is human-maintained, so its comments,
    LIMITS block, key order, and quoting survive. Only the TRACKING_ITEMS value
    is replaced — the table owns that key; the rest of the document is left
    exactly as the human wrote it.

    Called by table writers (the API write endpoints) AFTER a successful write —
    NOT off the NOTIFY signal. Records the written content's hash in
    WRITEBACK_GUARD so the file watcher skips the change it itself caused.
    """
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT market_hash_name, appid, item_nameid, stream,
                   currency, country, language, poll_interval_sec
            FROM tracked_items
            WHERE enabled = TRUE
            ORDER BY market_hash_name, stream
            """
        )
    finally:
        await conn.close()

    # Load the existing file with the round-trip loader so its comments/structure
    # are carried through. Only TRACKING_ITEMS is rebuilt from the table.
    path = Path(config_path)
    if path.exists():
        with open(path, "r") as f:
            doc = _yaml_rt.load(f) or {}
    else:
        doc = {}
    if "LIMITS" not in doc:
        doc["LIMITS"] = {"REQUESTS": 15, "WINDOW_SECONDS": 60}

    tracking_items = []
    for r in rows:
        item = {
            "market_hash_name": r["market_hash_name"],
            "appid": r["appid"],
            "currency": r["currency"],
            "country": r["country"],
            "language": r["language"],
            "polling-interval-in-seconds": r["poll_interval_sec"],
            "api_id": STREAM_TO_API_ID[r["stream"]],
        }
        if r["item_nameid"] is not None:
            item["item_nameid"] = r["item_nameid"]
        tracking_items.append(item)
    doc["TRACKING_ITEMS"] = tracking_items

    buf = io.StringIO()
    _yaml_rt.dump(doc, buf)
    content = buf.getvalue()

    # Record the hash BEFORE writing so the watcher (which may fire the instant
    # the file changes) already sees the guard set when it reads the file.
    WRITEBACK_GUARD.remember(content)
    with open(path, "w") as f:
        f.write(content)
