"""Pydantic response models for the read-path API.

These describe the JSON shapes the frontend consumes. Prices are already in
USD major units (the ingestion layer converts before storing), every
price-bearing point carries a `currency` field, and all timestamps serialize
as ISO 8601 UTC.

Where the ingestion-side models in src/dataClasses.py describe Steam's raw
wire format (formatted price strings, HTML blobs), they don't fit the read
path, so the models below are purpose-built for it. The JSONB order-book
arrays are passed through natively (see BookSnapshot).
"""

from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel, field_serializer

# The four streams the table accepts. priceoverview/histogram/activity are
# live snapshot pollers (snoozer); pricehistory is hourly archival (clockwork).
# All four are addable through these endpoints — the scheduler split is an
# internal dispatch detail, not a user-visible distinction.
VALID_STREAMS = ("priceoverview", "histogram", "activity", "pricehistory")
# Sane poll cadence bounds (seconds). Floor keeps a single item from blowing the
# budget on its own; ceiling stops typos like 99999999 sneaking in.
MIN_POLL_INTERVAL_SEC = 5
MAX_POLL_INTERVAL_SEC = 86_400

# pricehistory has no per-item cadence: clockwork runs it on a fixed :30 hourly
# tick regardless of this value. The client doesn't supply one; the endpoints
# stamp this canonical value so the NOT NULL column has an honest entry (3600 =
# the hour it actually runs on) and writeback to config stays meaningful.
PRICEHISTORY_POLL_SEC = 3600


class TrackedItemCreate(BaseModel):
    """Body for POST /tracked-items — add ONE item to the tracked set.

    Fields are loosely typed on purpose: semantic validation (stream value,
    interval bounds, etc.) happens in the endpoint so it can answer with a 400 +
    a clear message rather than Pydantic's 422 schema error. The writer is now
    untrusted (a browser), so nothing here is taken on faith.

    item_nameid is deliberately NOT accepted from the client: for
    histogram/activity it is resolved server-side from the item name, so a
    browser user never has to know it.
    """

    market_hash_name: str
    appid: int
    stream: str
    currency: int = 1
    # Optional: required for live streams, ignored for pricehistory (fixed hourly
    # cadence). The endpoint enforces presence for live streams with a clear 400.
    poll_interval_sec: Optional[int] = None
    country: str = "US"
    language: str = "english"


class TrackedItemPatch(BaseModel):
    """Body for PATCH /tracked-items — change cadence, stream, or enabled.

    The row is targeted by its real unique key (market_hash_name, stream), never
    by the internal autoincrement id. `stream` is the target disambiguator: an
    item can be tracked on several streams, so name alone can match more than one
    row. It is optional only so the endpoint can answer ambiguity with a clear
    409 (rather than Pydantic's 422) when a name matches multiple rows.

    The mutable fields are all optional; only the provided ones change. Set
    `new_stream` to move the row to a different stream. Validated in-endpoint
    (400 on bad values), same untrusted-writer stance as create.
    """

    market_hash_name: str
    stream: Optional[str] = None
    poll_interval_sec: Optional[int] = None
    new_stream: Optional[str] = None
    enabled: Optional[bool] = None


class TrackingAck(BaseModel):
    """Honest async-aware response: the write succeeded and polling will begin
    shortly — NOT that data exists yet. The frontend shows a 'collecting' state
    until points appear.
    """

    status: str
    market_hash_name: Optional[str] = None
    stream: Optional[str] = None
    note: Optional[str] = None
    # Current data via the SAME read function the matching GET uses, seeded into
    # the response so the frontend can prime its query cache without an extra
    # round-trip. Possibly an empty payload (just-tracked, no data yet). Shape
    # depends on the item's stream: OverviewResponse | BookSnapshot |
    # ActivityResponse.
    data: Optional[Any] = None


class TrackedItem(BaseModel):
    """One row of the tracked set, read from tracked_items (source of truth).

    stream and poll_interval_sec come from the backend authoritatively, so the
    frontend no longer guesses cadence/stream. currency is the ISO code mapped
    from the stored Steam currency id, for display.
    """

    market_hash_name: str
    appid: int
    item_nameid: Optional[int] = None
    stream: str
    currency: str
    poll_interval_sec: int


class RateLimitState(BaseModel):
    """Live (or configured) rate-limiter budget for the header."""

    used: Optional[int] = None  # None when not reachable cross-process
    limit: int
    window_seconds: int
    used_is_live: bool
    note: Optional[str] = None


class MetaResponse(BaseModel):
    """Operational state for the header (replaces the placeholder REQ 11/15)."""

    tracked_count: int
    rate_limit: RateLimitState
    last_ingest: Optional[datetime] = None


class PricePoint(BaseModel):
    """A single priceoverview snapshot."""

    timestamp: datetime
    currency: str
    lowest_price: Optional[float] = None
    median_price: Optional[float] = None
    volume: Optional[int] = None


class OverviewResponse(BaseModel):
    """Recent priceoverview snapshots for one item.

    currency is Optional so a tracked-but-empty item can return a 200 with an
    empty payload (currency=None, points=[]) instead of a 404.
    """

    currency: Optional[str] = None
    points: List[PricePoint] = []


class HistoryPoint(BaseModel):
    """A single historical (OHLC-style) price point."""

    timestamp: datetime
    currency: str
    price: float
    volume: int


class HistoryResponse(BaseModel):
    """Range-bounded price history for one item.

    currency is Optional so a tracked-but-empty item can return a 200 with an
    empty payload (currency=None, points=[]) instead of a 404, matching the
    other live read endpoints.
    """

    currency: Optional[str] = None
    points: List[HistoryPoint] = []


class BookSnapshot(BaseModel):
    """Latest order-book histogram snapshot for one item.

    The four nested columns are stored as JSONB and read back as native Python
    structures by the asyncpg codec, so they pass straight through as arrays
    (not strings): the order tables are lists of {price, quantity} objects and
    the graphs are arrays of [price, cumulative_quantity, label] triples.

    timestamp/currency are Optional so a tracked-but-empty item returns a 200
    with an empty payload (timestamp=None, everything null) rather than a 404.
    market_hash_name is always filled from the request path.
    """

    market_hash_name: str
    timestamp: Optional[datetime] = None
    currency: Optional[str] = None
    buy_order_table: Optional[List[Any]] = None
    sell_order_table: Optional[List[Any]] = None
    buy_order_graph: Optional[List[Any]] = None
    sell_order_graph: Optional[List[Any]] = None
    buy_order_count: Optional[int] = None
    sell_order_count: Optional[int] = None
    highest_buy_order: Optional[float] = None
    lowest_sell_order: Optional[float] = None


class TradeEvent(BaseModel):
    """One parsed trade-activity event."""

    timestamp: Optional[datetime] = None
    currency: Optional[str] = None
    action: Optional[str] = None
    price: Optional[float] = None

    @field_serializer("timestamp")
    def serialize_timestamp_as_utc(self, value: Optional[datetime]) -> Optional[str]:
        """Emit ISO 8601 UTC. Activity timestamps come from JSONB as naive
        strings (stored as naive UTC), so stamp them UTC before serializing."""
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()


class ActivityResponse(BaseModel):
    """Latest parsed trade activity for one item.

    currency is Optional so a tracked-but-empty item returns a 200 with an empty
    payload (currency=None, events=[]) instead of a 404.
    """

    currency: Optional[str] = None
    events: List[TradeEvent] = []
