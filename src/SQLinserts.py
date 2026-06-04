import json
import os
import asyncpg
from datetime import datetime
from typing import Optional
from src.dataClasses import (
    PriceOverviewData,
    OrdersHistogramData,
    OrdersActivityData,
    PriceHistoryData
)


class SQLinserts:
    """
    Manages data persistence in Postgres/TimescaleDB.

    Routes data objects to the appropriate table based on type.
    Uses match-case for maximum efficiency.
    """

    def __init__(
        self,
        timescale_dsn: Optional[str] = None,
        timescale_pool_min: int = 10,
        timescale_pool_max: int = 100
    ):
        """
        Initialize the database handler.

        Args:
            timescale_dsn: PostgreSQL connection string for TimescaleDB
                          (e.g., "postgresql://user:pass@localhost/dbname").
                          REQUIRED — Postgres is the only backend. A missing DSN
                          raises immediately so a misconfigured env fails loudly
                          instead of silently writing nowhere.
            timescale_pool_min: Minimum connections in TimescaleDB pool (default: 10)
            timescale_pool_max: Maximum connections in TimescaleDB pool (default: 100)
        """
        if not timescale_dsn:
            raise ValueError(
                "timescale_dsn is required (set CS2_PG_DSN in .env). "
                "Postgres is the only backend; there is no SQLite fallback."
            )
        self.timescale_dsn = timescale_dsn
        self.pg_pool_min = timescale_pool_min
        self.pg_pool_max = timescale_pool_max
        # Single shared Postgres/Timescale pool for ALL tables: the price_history
        # hypertable AND the three live snapshot tables (price_overview,
        # orders_histogram, orders_activity). One pool, one database.
        self.pg_pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        """Open the Postgres pool and create schemas."""
        await self._initialize_timescale()

    async def close(self):
        """Close the Postgres pool."""
        if self.pg_pool:
            await self.pg_pool.close()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def store_data(
        self,
        data: PriceOverviewData | OrdersHistogramData | OrdersActivityData | PriceHistoryData,
        item_config: dict
    ) -> Optional[str]:
        """
        Route data to appropriate database based on type.

        Args:
            data: Pydantic data object from API client
            item_config: Item configuration dict with market_hash_name, appid, etc.

        Returns:
            The ISO currency code derived from Steam's response and stored on the
            row(s) (e.g. "USD", "INR"). Callers use this to align ingest defaults
            with the wallet's actual currency. None if it couldn't be determined.
        """
        # Match-case for MAXIMUM EFFICIENCY
        match data:
            case PriceOverviewData():
                return await self._store_price_overview(data, item_config)
            case OrdersHistogramData():
                return await self._store_histogram(data, item_config)
            case OrdersActivityData():
                return await self._store_activity(data, item_config)
            case PriceHistoryData():
                return await self._store_price_history(data, item_config)
            case _:
                raise ValueError(f"Unknown data type: {type(data)}")

    async def fetch_price_history_last_timestamps(self) -> dict:
        """
        Return {market_hash_name: newest stored point time} for price_history.

        Used by the bulk collector to decide what to skip. An item is only "done"
        if its newest point is recent enough — items with months-old data are
        stale and must be re-fetched (the per-point delta dedup makes that cheap,
        inserting only the new tail). Returning the last timestamp (not just the
        name) lets the collector apply a staleness window instead of blindly
        skipping anything that has rows.

        Timescale returns datetime objects.
        """
        async with self.pg_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT market_hash_name, MAX(time) AS last_time
                FROM price_history GROUP BY market_hash_name
            """)
            return {row['market_hash_name']: row['last_time'] for row in rows}

    # ========================================================================
    # TimescaleDB Initialization
    # ========================================================================

    async def _initialize_timescale(self):
        """Create the shared Postgres pool and all Postgres-backed tables.

        Builds the single pool used by every Postgres table — the price_history
        hypertable and the three live snapshot tables. Each new connection
        registers a JSONB codec so Python dicts/lists pass through to JSONB
        columns directly and come back as structured objects (not strings).
        """
        self.pg_pool = await asyncpg.create_pool(
            self.timescale_dsn,
            min_size=self.pg_pool_min,
            max_size=self.pg_pool_max,
            command_timeout=60,  # 60 second query timeout
            max_inactive_connection_lifetime=300,  # 5 minute idle connection lifetime
            init=self._register_jsonb_codec
        )
        await self._create_timescale_tables()
        await self._create_live_tables()

    async def _register_jsonb_codec(self, conn):
        """Make asyncpg encode/decode JSONB as native Python objects.

        asyncpg does NOT auto-encode dicts to JSONB by default. Registering this
        codec on every pooled connection lets the live-table inserts pass Python
        lists/dicts straight into JSONB columns, and reads return parsed JSON
        instead of raw strings — so we can aggregate inside the JSON in SQL later.
        """
        await conn.set_type_codec(
            'jsonb',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )

    async def _create_live_tables(self):
        """Create the three live snapshot tables in Postgres.

        Postgres types throughout: BIGSERIAL keys,
        TIMESTAMPTZ timestamps defaulting to NOW(), DOUBLE PRECISION prices, and
        JSONB for the genuinely-nested columns (order tables/graphs, activity
        blobs) so they're queryable in SQL. Scalar columns stay plain types.
        """
        async with self.pg_pool.acquire() as conn:
            # Price Overview - current market prices (single-row snapshots)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS price_overview (
                    id BIGSERIAL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    appid INTEGER NOT NULL,
                    market_hash_name TEXT NOT NULL,
                    item_nameid INTEGER,
                    currency TEXT NOT NULL,
                    country TEXT NOT NULL,
                    language TEXT NOT NULL,
                    lowest_price DOUBLE PRECISION,
                    median_price DOUBLE PRECISION,
                    volume INTEGER,
                    PRIMARY KEY (id, timestamp)
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_overview_item_time
                ON price_overview(market_hash_name, timestamp DESC)
            """)

            # Orders Histogram - order book snapshots (nested tables/graphs as JSONB)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS orders_histogram (
                    id BIGSERIAL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    appid INTEGER NOT NULL,
                    market_hash_name TEXT NOT NULL,
                    item_nameid INTEGER NOT NULL,
                    currency TEXT NOT NULL,
                    country TEXT NOT NULL,
                    language TEXT NOT NULL,
                    buy_order_table JSONB,
                    sell_order_table JSONB,
                    buy_order_graph JSONB,
                    sell_order_graph JSONB,
                    buy_order_count INTEGER,
                    sell_order_count INTEGER,
                    highest_buy_order DOUBLE PRECISION,
                    lowest_sell_order DOUBLE PRECISION,
                    PRIMARY KEY (id, timestamp)
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_histogram_item_time
                ON orders_histogram(market_hash_name, timestamp DESC)
            """)

            # Orders Activity - trade activity log (raw + parsed blobs as JSONB)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS orders_activity (
                    id BIGSERIAL,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    appid INTEGER NOT NULL,
                    market_hash_name TEXT NOT NULL,
                    item_nameid INTEGER NOT NULL,
                    currency TEXT NOT NULL,
                    country TEXT NOT NULL,
                    language TEXT NOT NULL,
                    activity_raw JSONB,
                    parsed_activities JSONB,
                    activity_count INTEGER,
                    steam_timestamp BIGINT NOT NULL,
                    PRIMARY KEY (id, timestamp)
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_activity_item_time
                ON orders_activity(market_hash_name, timestamp DESC)
            """)

            # ----------------------------------------------------------------
            # Convert the three live tables to Timescale hypertables.
            #
            # All partition on their 'timestamp' column. The composite PK
            # (id, timestamp) above exists specifically so these conversions
            # are legal — Timescale requires every unique/primary key to
            # include the partitioning column.
            #
            # Policies are deliberate, NOT uniform:
            #   - orders_histogram / orders_activity: a high-frequency firehose.
            #     Old depth snapshots and tape have little value, so retain
            #     only 30 days and compress aggressively (after 1 day) so most
            #     of the retained window sits compressed.
            #   - price_overview: price-over-time, smaller per row, a candidate
            #     to fold into long-term history later — so NO retention, just
            #     compression after 7 days.
            # All segment compression by market_hash_name, matching
            # price_history, so per-item scans stay cheap when compressed.
            # ----------------------------------------------------------------
            for table in ("price_overview", "orders_histogram", "orders_activity"):
                await conn.execute(
                    "SELECT create_hypertable($1, 'timestamp', if_not_exists => TRUE)",
                    table,
                )
                await conn.execute(f"""
                    ALTER TABLE {table} SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'market_hash_name'
                    )
                """)

            # price_overview: compress after 7 days, NO retention (keep growing).
            await conn.execute("""
                SELECT add_compression_policy('price_overview',
                    INTERVAL '7 days', if_not_exists => TRUE)
            """)

            # Firehose tables: compress after 1 day, retain only 30 days.
            for table in ("orders_histogram", "orders_activity"):
                await conn.execute(
                    "SELECT add_compression_policy($1, INTERVAL '1 day', if_not_exists => TRUE)",
                    table,
                )
                await conn.execute(
                    "SELECT add_retention_policy($1, INTERVAL '30 days', if_not_exists => TRUE)",
                    table,
                )

    async def _create_timescale_tables(self):
        """Create TimescaleDB hypertable for price history."""
        async with self.pg_pool.acquire() as conn:
            # Create table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    time TIMESTAMPTZ NOT NULL,
                    appid INTEGER NOT NULL,
                    market_hash_name TEXT NOT NULL,
                    item_nameid INTEGER,
                    currency TEXT NOT NULL,
                    country TEXT NOT NULL,
                    language TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    volume INTEGER NOT NULL,
                    fetched_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (market_hash_name, time)
                )
            """)

            # Check if already a hypertable
            is_hypertable = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = 'price_history'
                )
            """)

            if not is_hypertable:
                # Convert to hypertable
                await conn.execute("""
                    SELECT create_hypertable('price_history', 'time',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    )
                """)

                # Add compression policy (compress data older than 7 days)
                await conn.execute("""
                    ALTER TABLE price_history SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'market_hash_name'
                    )
                """)

                await conn.execute("""
                    SELECT add_compression_policy('price_history',
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    )
                """)

                # Add retention policy (delete data older than 90 days)
                await conn.execute("""
                    SELECT add_retention_policy('price_history',
                        INTERVAL '90 days',
                        if_not_exists => TRUE
                    )
                """)

    # ========================================================================
    # Live Storage Methods (Postgres)
    # ========================================================================

    async def _store_price_overview(self, data: PriceOverviewData, item_config: dict):
        """Store a price overview snapshot."""
        return await self._store_price_overview_postgres(data, item_config)

    async def _store_histogram(self, data: OrdersHistogramData, item_config: dict):
        """Store an order book histogram snapshot."""
        return await self._store_histogram_postgres(data, item_config)

    async def _store_activity(self, data: OrdersActivityData, item_config: dict):
        """Store a trade activity snapshot."""
        return await self._store_activity_postgres(data, item_config)

    # ------------------------------------------------------------------------
    # Postgres live storage
    # ------------------------------------------------------------------------

    async def _store_price_overview_postgres(self, data: PriceOverviewData, item_config: dict):
        """Insert a single price overview snapshot into Postgres."""
        lowest_price_float = self._parse_steam_price(data.lowest_price)
        median_price_float = self._parse_steam_price(data.median_price)
        volume_int = self._parse_volume(data.volume)
        currency = self._extract_currency(data.lowest_price or data.median_price or "") or 'USD'

        async with self.pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO price_overview (
                    appid, market_hash_name, item_nameid, currency, country, language,
                    lowest_price, median_price, volume
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                item_config['appid'],
                item_config['market_hash_name'],
                item_config.get('item_nameid'),
                currency,
                item_config.get('country', 'US'),
                item_config.get('language', 'english'),
                lowest_price_float,
                median_price_float,
                volume_int
            )
        return currency

    async def _store_histogram_postgres(self, data: OrdersHistogramData, item_config: dict):
        """Insert a single order book histogram snapshot into Postgres.

        The four nested columns go into JSONB. The registered JSONB codec encodes
        Python lists directly, so we pass the structures through (no json.dumps).
        """
        # Nested structures → JSONB (codec serializes these dicts/lists for us)
        buy_orders = [order.model_dump() for order in data.buy_order_table] if data.buy_order_table else None
        sell_orders = [order.model_dump() for order in data.sell_order_table] if data.sell_order_table else None
        buy_graph = data.buy_order_graph if data.buy_order_graph else None
        sell_graph = data.sell_order_graph if data.sell_order_graph else None

        # Scalar numeric fields
        # Counts: parse via _parse_volume so thousands-separated values ("1,234")
        # don't get dropped to NULL the way .isdigit() did (the buy-side bug).
        buy_count = self._parse_volume(str(data.buy_order_count)) if data.buy_order_count is not None else None
        sell_count = self._parse_volume(str(data.sell_order_count)) if data.sell_order_count is not None else None
        # These two are Steam integer cents -> major units (see helper); do NOT
        # use _parse_steam_price here (it would leave them 100x too large).
        highest_buy = self._convert_steam_order_price_to_major_units(data.highest_buy_order)
        lowest_sell = self._convert_steam_order_price_to_major_units(data.lowest_sell_order)

        currency = self._extract_currency(data.price_suffix) or self._extract_currency(data.buy_order_price or "") or 'USD'

        async with self.pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO orders_histogram (
                    appid, market_hash_name, item_nameid, currency, country, language,
                    buy_order_table, sell_order_table,
                    buy_order_graph, sell_order_graph,
                    buy_order_count, sell_order_count,
                    highest_buy_order, lowest_sell_order
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            """,
                item_config['appid'],
                item_config['market_hash_name'],
                item_config['item_nameid'],
                currency,
                item_config.get('country', 'US'),
                item_config.get('language', 'english'),
                buy_orders,
                sell_orders,
                buy_graph,
                sell_graph,
                buy_count,
                sell_count,
                highest_buy,
                lowest_sell
            )
        return currency

    async def _store_activity_postgres(self, data: OrdersActivityData, item_config: dict):
        """Insert a single trade activity snapshot into Postgres.

        activity_raw and parsed_activities go into JSONB. parsed activities are
        dumped via model_dump(mode='json') first so datetimes become ISO strings,
        then the JSONB codec serializes the resulting list.
        """
        # Raw HTML activity strings → JSONB array
        activity_raw = data.activity if data.activity else None

        # Parsed activities → JSONB (model_dump(mode='json') normalizes datetimes)
        if data.parsed_activities:
            parsed_activities = [activity.model_dump(mode='json') for activity in data.parsed_activities]
        else:
            parsed_activities = None

        activity_count = len(data.parsed_activities) if data.parsed_activities else 0

        # Extract currency from first parsed activity price (if available)
        currency = None
        if data.parsed_activities and len(data.parsed_activities) > 0:
            first_price = data.parsed_activities[0].price
            currency = self._extract_currency(first_price)
        currency = currency or 'USD'

        async with self.pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO orders_activity (
                    appid, market_hash_name, item_nameid, currency, country, language,
                    activity_raw, parsed_activities,
                    activity_count, steam_timestamp
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                item_config['appid'],
                item_config['market_hash_name'],
                item_config['item_nameid'],
                currency,
                item_config.get('country', 'US'),
                item_config.get('language', 'english'),
                activity_raw,
                parsed_activities,
                activity_count,
                data.timestamp
            )
        return currency

    # ========================================================================
    # TimescaleDB Storage Methods
    # ========================================================================

    async def _store_price_history(self, data: PriceHistoryData, item_config: dict):
        """
        Store price history to TimescaleDB.

        Loops through all price points in data.prices and inserts individually
        with UPSERT to avoid duplicates.
        """
        return await self._store_price_history_timescale(data, item_config)

    async def _store_price_history_timescale(self, data: PriceHistoryData, item_config: dict):
        """
        Insert price history points into TimescaleDB hypertable.

        Only inserts NEW points (after the most recent timestamp we already have).
        Initial run inserts all points; subsequent runs insert only the delta.
        """
        market_hash_name = item_config['market_hash_name']

        # Query the most recent timestamp we have for this item
        async with self.pg_pool.acquire() as conn:
            last_timestamp = await conn.fetchval("""
                SELECT MAX(time) FROM price_history WHERE market_hash_name = $1
            """, market_hash_name)

        # Extract currency from price_prefix or price_suffix
        currency = self._extract_currency(data.price_suffix) or self._extract_currency(data.price_prefix) or 'USD'

        # Steam returns data in ascending order (oldest → newest)
        # Iterate in reverse to find new points at the end, stop when we hit existing data
        records = []
        for price_point in reversed(data.prices):
            # price_point is [date_string, price_float, volume_string]
            date_string, price, volume = price_point

            # Parse Steam's datetime format to proper timestamp
            parsed_time = self._parse_steam_datetime(date_string)
            if not parsed_time:
                continue  # Skip invalid dates

            # Stop when we reach data we already have
            if last_timestamp and parsed_time <= last_timestamp.replace(tzinfo=None):
                break

            # Parse volume to integer
            volume_int = self._parse_volume(volume)
            if volume_int is None:
                volume_int = 0

            records.append((
                parsed_time,
                item_config['appid'],
                market_hash_name,
                item_config.get('item_nameid'),
                currency,
                item_config.get('country', 'US'),
                item_config.get('language', 'english'),
                float(price),
                volume_int
            ))

        if not records:
            print(f"  ✓ {market_hash_name}: up to date")
            return currency

        # Reverse to restore chronological order for insert
        records.reverse()

        # Batch insert with chunking to avoid memory issues
        BATCH_SIZE = 100
        async with self.pg_pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    await conn.executemany("""
                        INSERT INTO price_history (
                            time, appid, market_hash_name, item_nameid, currency, country, language, price, volume
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (market_hash_name, time) DO NOTHING
                    """, batch)

        print(f"  ✓ {market_hash_name}: {len(records)} new historical price points")
        return currency

    # ========================================================================
    # Utility Methods - Parse Steam's formatted strings
    # ========================================================================

    def _parse_steam_price(self, price_str: Optional[str]) -> Optional[float]:
        """
        Parse Steam's formatted price string to float.

        Examples:
            "0,03€" -> 0.03
            "$5.00" -> 5.0
            "1.234,56€" -> 1234.56
            None -> None
        """
        if not price_str:
            return None

        try:
            # Remove currency symbols and whitespace
            cleaned = price_str.strip()
            for symbol in ['$', '€', '£', '¥', '₹', '₽', 'pуб.', 'R$', 'CDN$', 'A$', 'HK$', 'S$', '₩', '₴', 'CHF', 'kr', 'zł', 'R', '฿']:
                cleaned = cleaned.replace(symbol, '')

            cleaned = cleaned.strip()

            # Handle European format (1.234,56) vs US format (1,234.56)
            if ',' in cleaned and '.' in cleaned:
                # Both present - determine which is decimal separator
                comma_pos = cleaned.rfind(',')
                dot_pos = cleaned.rfind('.')
                if comma_pos > dot_pos:
                    # European format: 1.234,56
                    cleaned = cleaned.replace('.', '').replace(',', '.')
                else:
                    # US format: 1,234.56
                    cleaned = cleaned.replace(',', '')
            elif ',' in cleaned:
                # Only comma - check if it's thousands or decimal
                # If last comma has 2 digits after, it's decimal
                parts = cleaned.split(',')
                if len(parts[-1]) == 2:
                    # Decimal separator: 0,03
                    cleaned = cleaned.replace(',', '.')
                else:
                    # Thousands separator: 1,000
                    cleaned = cleaned.replace(',', '')

            return float(cleaned)
        except (ValueError, AttributeError):
            return None

    def _convert_steam_order_price_to_major_units(self, cents_str) -> Optional[float]:
        """Convert Steam's order-book price (integer minor units) to major units.

        The itemordershistogram endpoint returns highest_buy_order and
        lowest_sell_order as a separator-less integer in the currency's minor
        unit (e.g. "6711" = 67.11, "177" = 1.77). _parse_steam_price would read
        "6711" as 6711.0, leaving these two scalars 100x too large and out of
        scale with price_overview's major-unit prices. Divide by 100 so the two
        tables agree. ONLY for these scalar fields — the JSONB order-table price
        strings ("67,11€") already parse correctly via _parse_steam_price.
        """
        if cents_str is None or cents_str == "":
            return None
        try:
            return int(str(cents_str).replace(',', '')) / 100.0
        except (ValueError, AttributeError):
            return None

    def _parse_volume(self, volume_str: Optional[str]) -> Optional[int]:
        """
        Parse Steam's volume string to integer.

        Examples:
            "435" -> 435
            "1,234" -> 1234
            None -> None
        """
        if not volume_str:
            return None

        try:
            # Remove commas (thousands separator)
            cleaned = volume_str.replace(',', '').replace('.', '')
            return int(cleaned)
        except (ValueError, AttributeError):
            return None

    def _extract_currency(self, price_str: str) -> Optional[str]:
        """
        Extract currency code from Steam's price string.

        Maps currency symbols to ISO 4217 codes.
        Returns None if currency cannot be determined.
        """
        if not price_str:
            return None

        # Currency symbol to ISO code mapping
        currency_map = {
            '$': 'USD',
            '€': 'EUR',
            '£': 'GBP',
            '¥': 'JPY',
            '₹': 'INR',
            '₽': 'RUB',
            'pуб.': 'RUB',
            'R$': 'BRL',
            'CDN$': 'CAD',
            'A$': 'AUD',
            'HK$': 'HKD',
            'S$': 'SGD',
            '₩': 'KRW',
            '₴': 'UAH',
            'CHF': 'CHF',
            'kr': 'SEK',  # Could also be NOK or DKK
            'zł': 'PLN',
            'R': 'ZAR',
            '฿': 'THB',
        }

        for symbol, code in currency_map.items():
            if symbol in price_str:
                return code

        return None

    def _parse_steam_datetime(self, date_str: str) -> Optional[datetime]:
        """
        Parse Steam's datetime format to Python datetime.

        Examples:
            "Jul 02 2014 01: +0" -> datetime(2014, 7, 2, 1, 0, tzinfo=UTC)
            "Dec 25 2023 14: +0" -> datetime(2023, 12, 25, 14, 0, tzinfo=UTC)

        Steam's format: "MMM DD YYYY HH: +TZ"
        The "+0" is UTC offset (usually +0 for UTC)
        """
        if not date_str:
            return None

        try:
            # Steam format: "Jul 02 2014 01: +0"
            # Split by space to handle the weird ": +0" format
            parts = date_str.strip().split()

            if len(parts) >= 4:
                # Parts: ['Jul', '02', '2014', '01:', '+0']
                month = parts[0]
                day = parts[1]
                year = parts[2]
                hour = parts[3].rstrip(':')  # Remove trailing colon

                # Reconstruct without timezone (assume UTC)
                clean_str = f"{month} {day} {year} {hour}"

                # Parse to datetime
                dt = datetime.strptime(clean_str, "%b %d %Y %H")

                # Return as UTC (Steam uses UTC for price history)
                return dt.replace(tzinfo=None)  # naive UTC; the timescale insert path treats it as UTC

            return None
        except (ValueError, IndexError, AttributeError):
            return None


# ============================================================================
# Async context manager usage example
# ============================================================================

async def example_usage():
    """Example of how schedulers will use DataWizard."""
    async with SQLinserts(
        timescale_dsn=os.getenv("CS2_PG_DSN")  # Required; loaded from .env
    ) as wizard:
        # After fetching data from API:
        # result = await client.fetch_price_overview(...)
        # await wizard.store_data(result, item_config)
        pass
