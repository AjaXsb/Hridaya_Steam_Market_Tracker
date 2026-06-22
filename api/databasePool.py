"""asyncpg connection pool for the API process.

Mirrors the pool pattern in src/SQLinserts.py (shared pool, JSONB codec so
nested columns come back as native Python objects). The DSN comes from the same
CS2_PG_DSN env var the ingestion side uses. Connections are pinned to UTC so
TIMESTAMPTZ values serialize as ISO 8601 UTC.

Originally read-only: it opens no schemas and the read endpoints never write.
The tracked-set write endpoints (POST/PATCH/DELETE /tracked-items) now write
to tracked_items through this same pool, so it is no longer read-only in
practice — the connection role simply has whatever rights CS2_PG_DSN grants.

FUTURE HARDENING (not this pass): give the API a least-privilege role —
read on the data tables, write only on tracked_items — instead of sharing the
full-rights ingestion DSN. That's a deployment/credentials change, deliberately
out of scope here.
"""

import json
import os
from typing import Optional

import asyncpg


async def register_jsonb_codec(conn: asyncpg.Connection) -> None:
    """Decode JSONB as native Python objects on each pooled connection.

    Without this, asyncpg hands JSONB columns back as raw strings. Registering
    the codec means the order-book tables/graphs read back as structured
    lists/dicts and pass straight through to the JSON response.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def open_read_pool() -> asyncpg.Pool:
    """Create the shared API pool from CS2_PG_DSN.

    Name kept for compatibility; the pool now serves both the read endpoints and
    the tracked-set writes. Raises immediately if the DSN is missing so a
    misconfigured environment fails loudly at startup rather than on first request.
    """
    dsn = os.getenv("CS2_PG_DSN")
    if not dsn:
        raise ValueError(
            "CS2_PG_DSN is required (set it in .env). The API connects to the "
            "same Postgres/Timescale instance as ingestion."
        )
    return await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=10,
        command_timeout=60,
        max_inactive_connection_lifetime=300,
        server_settings={"timezone": "UTC"},
        init=register_jsonb_codec,
    )


class PoolHolder:
    """Holds the single process-wide pool, opened once at startup."""

    pool: Optional[asyncpg.Pool] = None


holder = PoolHolder()
