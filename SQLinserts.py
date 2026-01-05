import json
import aiosqlite
import asyncpg
from datetime import datetime
from typing import Optional
from dataClasses import (
    PriceOverviewData,
    OrdersHistogramData,
    OrdersActivityData,
    PriceHistoryData
)


class SQLinserts:
    """
    Manages data persistence across SQLite and TimescaleDB.

    Routes data objects to the appropriate database based on type.
    Uses match-case for maximum efficiency.
    """

    def __init__(
        self,
        sqlite_path: str = "market_data.db",
        timescale_dsn: Optional[str] = None,
        timescale_pool_min: int = 10,
        timescale_pool_max: int = 100
    ):
        """
        Initialize database connections.

        Args:
            sqlite_path: Path to SQLite database file
            timescale_dsn: PostgreSQL connection string for TimescaleDB
                          (e.g., "postgresql://user:pass@localhost/dbname")
                          If None, price_history will also use SQLite
            timescale_pool_min: Minimum connections in TimescaleDB pool (default: 10)
            timescale_pool_max: Maximum connections in TimescaleDB pool (default: 100)
        """
        self.sqlite_path = sqlite_path
        self.timescale_dsn = timescale_dsn
        self.timescale_pool_min = timescale_pool_min
        self.timescale_pool_max = timescale_pool_max
        self.sqlite_conn: Optional[aiosqlite.Connection] = None
        self.timescale_pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        """Initialize database connections and create schemas."""
        await self._initialize_sqlite()
        if self.timescale_dsn:
            await self._initialize_timescale()

    async def close(self):
        """Close all database connections."""
        if self.sqlite_conn:
            await self.sqlite_conn.close()
        if self.timescale_pool:
            await self.timescale_pool.close()

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
    ):
        """
        Route data to appropriate database based on type.

        Args:
            data: Pydantic data object from API client
            item_config: Item configuration dict with market_hash_name, appid, etc.
        """
        # Match-case for MAXIMUM EFFICIENCY
        match data:
            case PriceOverviewData():
                await self._store_price_overview(data, item_config)
            case OrdersHistogramData():
                await self._store_histogram(data, item_config)
            case OrdersActivityData():
                await self._store_activity(data, item_config)
            case PriceHistoryData():
                await self._store_price_history(data, item_config)
            case _:
                raise ValueError(f"Unknown data type: {type(data)}")

    # ========================================================================
    # SQLite Initialization
    # ========================================================================

    async def _initialize_sqlite(self):
        """Create SQLite connection and tables for operational data."""
        self.sqlite_conn = await aiosqlite.connect(self.sqlite_path)

        # Performance optimizations for SQLite
        await self.sqlite_conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for concurrency
        await self.sqlite_conn.execute("PRAGMA synchronous=NORMAL")  # Balance safety vs speed
        await self.sqlite_conn.execute("PRAGMA cache_size=-64000")  # 64MB cache (negative = KB)
        await self.sqlite_conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in RAM
        await self.sqlite_conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
        await self.sqlite_conn.execute("PRAGMA page_size=4096")  # Optimal page size for modern systems

        # Create tables
        await self._create_sqlite_tables()

    async def _create_sqlite_tables(self):
        """Create SQLite schema for operational snapshots."""
        assert self.sqlite_conn is not None, "SQLite connection not initialized"

        # Price Overview - current market prices
        await self.sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS price_overview (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                appid INTEGER NOT NULL,
                market_hash_name TEXT NOT NULL,
                item_nameid INTEGER,
                currency TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT NOT NULL,
                lowest_price REAL,
                median_price REAL,
                volume INTEGER
            )
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overview_item_time
            ON price_overview(market_hash_name, timestamp DESC)
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overview_timestamp
            ON price_overview(timestamp DESC)
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_overview_appid
            ON price_overview(appid, market_hash_name, timestamp DESC)
        """)

        # Orders Histogram - order book snapshots
        await self.sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS orders_histogram (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                appid INTEGER NOT NULL,
                market_hash_name TEXT NOT NULL,
                item_nameid INTEGER NOT NULL,
                currency TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT NOT NULL,
                buy_order_table TEXT,
                sell_order_table TEXT,
                buy_order_graph TEXT,
                sell_order_graph TEXT,
                buy_order_count INTEGER,
                sell_order_count INTEGER,
                highest_buy_order REAL,
                lowest_sell_order REAL
            )
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_histogram_item_time
            ON orders_histogram(market_hash_name, timestamp DESC)
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_histogram_timestamp
            ON orders_histogram(timestamp DESC)
        """)

        # Orders Activity - trade activity log
        await self.sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS orders_activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                appid INTEGER NOT NULL,
                market_hash_name TEXT NOT NULL,
                item_nameid INTEGER NOT NULL,
                currency TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT NOT NULL,
                activity_raw TEXT,
                parsed_activities TEXT,
                activity_count INTEGER,
                steam_timestamp INTEGER NOT NULL
            )
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_activity_item_time
            ON orders_activity(market_hash_name, timestamp DESC)
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_activity_timestamp
            ON orders_activity(timestamp DESC)
        """)

        await self.sqlite_conn.commit()

    # ========================================================================
    # TimescaleDB Initialization
    # ========================================================================

    async def _initialize_timescale(self):
        """Create TimescaleDB connection pool and hypertable."""
        self.timescale_pool = await asyncpg.create_pool(
            self.timescale_dsn,
            min_size=self.timescale_pool_min,
            max_size=self.timescale_pool_max,
            command_timeout=60,  # 60 second query timeout
            max_inactive_connection_lifetime=300  # 5 minute idle connection lifetime
        )
        await self._create_timescale_tables()

    async def _create_timescale_tables(self):
        """Create TimescaleDB hypertable for price history."""
        async with self.timescale_pool.acquire() as conn:
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
    # SQLite Storage Methods
    # ========================================================================

    async def _store_price_overview(self, data: PriceOverviewData, item_config: dict):
        """Store price overview snapshot to SQLite."""
        assert self.sqlite_conn is not None, "SQLite connection not initialized"

        # Parse prices and extract currency
        lowest_price_float = self._parse_steam_price(data.lowest_price)
        median_price_float = self._parse_steam_price(data.median_price)
        volume_int = self._parse_volume(data.volume)
        currency = self._extract_currency(data.lowest_price or data.median_price or "") or 'USD'

        await self.sqlite_conn.execute("""
            INSERT INTO price_overview (
                appid, market_hash_name, item_nameid, currency, country, language,
                lowest_price, median_price, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item_config['appid'],
            item_config['market_hash_name'],
            item_config.get('item_nameid'),
            currency,
            item_config.get('country', 'US'),
            item_config.get('language', 'english'),
            lowest_price_float,
            median_price_float,
            volume_int
        ))
        await self.sqlite_conn.commit()

    async def _store_histogram(self, data: OrdersHistogramData, item_config: dict):
        """Store order book histogram snapshot to SQLite."""
        assert self.sqlite_conn is not None, "SQLite connection not initialized"

        # Convert order tables to JSON (keep original structure with price/quantity)
        buy_orders_json = json.dumps([order.model_dump() for order in data.buy_order_table]) if data.buy_order_table else None
        sell_orders_json = json.dumps([order.model_dump() for order in data.sell_order_table]) if data.sell_order_table else None

        # Convert graph data to JSON (arrays of [price, quantity, label])
        buy_graph_json = json.dumps(data.buy_order_graph) if data.buy_order_graph else None
        sell_graph_json = json.dumps(data.sell_order_graph) if data.sell_order_graph else None

        # Parse numeric fields
        buy_count = int(data.buy_order_count) if isinstance(data.buy_order_count, str) and data.buy_order_count.isdigit() else (data.buy_order_count if isinstance(data.buy_order_count, int) else None)
        sell_count = int(data.sell_order_count) if isinstance(data.sell_order_count, str) and data.sell_order_count.isdigit() else (data.sell_order_count if isinstance(data.sell_order_count, int) else None)
        highest_buy = self._parse_steam_price(data.highest_buy_order)
        lowest_sell = self._parse_steam_price(data.lowest_sell_order)

        # Extract currency from price_suffix or buy_order_price
        currency = self._extract_currency(data.price_suffix) or self._extract_currency(data.buy_order_price or "") or 'USD'

        await self.sqlite_conn.execute("""
            INSERT INTO orders_histogram (
                appid, market_hash_name, item_nameid, currency, country, language,
                buy_order_table, sell_order_table,
                buy_order_graph, sell_order_graph,
                buy_order_count, sell_order_count,
                highest_buy_order, lowest_sell_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item_config['appid'],
            item_config['market_hash_name'],
            item_config['item_nameid'],
            currency,
            item_config.get('country', 'US'),
            item_config.get('language', 'english'),
            buy_orders_json,
            sell_orders_json,
            buy_graph_json,
            sell_graph_json,
            buy_count,
            sell_count,
            highest_buy,
            lowest_sell
        ))
        await self.sqlite_conn.commit()

    async def _store_activity(self, data: OrdersActivityData, item_config: dict):
        """Store trade activity snapshot to SQLite."""
        assert self.sqlite_conn is not None, "SQLite connection not initialized"

        # Store raw HTML activity as JSON array of strings
        activity_raw_json = json.dumps(data.activity) if data.activity else None

        # Convert parsed activities to JSON (serialize datetime properly)
        if data.parsed_activities:
            parsed_json = json.dumps([
                activity.model_dump(mode='json') for activity in data.parsed_activities
            ], default=str)  # Fallback for any remaining non-serializable types
        else:
            parsed_json = None

        # Count activities
        activity_count = len(data.parsed_activities) if data.parsed_activities else 0

        # Extract currency from first parsed activity price (if available)
        currency = None
        if data.parsed_activities and len(data.parsed_activities) > 0:
            first_price = data.parsed_activities[0].price
            currency = self._extract_currency(first_price)

        currency = currency or 'USD'

        await self.sqlite_conn.execute("""
            INSERT INTO orders_activity (
                appid, market_hash_name, item_nameid, currency, country, language,
                activity_raw, parsed_activities,
                activity_count, steam_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item_config['appid'],
            item_config['market_hash_name'],
            item_config['item_nameid'],
            currency,
            item_config.get('country', 'US'),
            item_config.get('language', 'english'),
            activity_raw_json,
            parsed_json,
            activity_count,
            data.timestamp
        ))
        await self.sqlite_conn.commit()

    # ========================================================================
    # TimescaleDB Storage Methods
    # ========================================================================

    async def _store_price_history(self, data: PriceHistoryData, item_config: dict):
        """
        Store price history to TimescaleDB (or SQLite if TimescaleDB not configured).

        Loops through all price points in data.prices and inserts individually
        with UPSERT to avoid duplicates.
        """
        if self.timescale_pool:
            await self._store_price_history_timescale(data, item_config)
        else:
            await self._store_price_history_sqlite(data, item_config)

    async def _store_price_history_timescale(self, data: PriceHistoryData, item_config: dict):
        """
        Insert price history points into TimescaleDB hypertable.

        Only inserts NEW points (after the most recent timestamp we already have).
        Initial run inserts all points; subsequent runs insert only the delta.
        """
        market_hash_name = item_config['market_hash_name']

        # Query the most recent timestamp we have for this item
        async with self.timescale_pool.acquire() as conn:
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
            return

        # Reverse to restore chronological order for insert
        records.reverse()

        # Batch insert with chunking to avoid memory issues
        BATCH_SIZE = 100
        async with self.timescale_pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    await conn.executemany("""
                        INSERT INTO price_history (
                            time, appid, market_hash_name, item_nameid, currency, country, language, price, volume
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (market_hash_name, time) DO NOTHING
                    """, batch)

        print(f"  ✓ {market_hash_name}: {len(records)} new points")

    async def _store_price_history_sqlite(self, data: PriceHistoryData, item_config: dict):
        """
        Fallback: Store price history in SQLite if TimescaleDB not available.

        Only inserts NEW points (after the most recent timestamp we already have).
        Initial run inserts all points; subsequent runs insert only the delta.
        """
        assert self.sqlite_conn is not None, "SQLite connection not initialized"

        market_hash_name = item_config['market_hash_name']

        # Create table if not exists
        await self.sqlite_conn.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time DATETIME NOT NULL,
                appid INTEGER NOT NULL,
                market_hash_name TEXT NOT NULL,
                item_nameid INTEGER,
                currency TEXT NOT NULL,
                country TEXT NOT NULL,
                language TEXT NOT NULL,
                price REAL NOT NULL,
                volume INTEGER NOT NULL,
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(market_hash_name, time)
            )
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_history_item_time
            ON price_history(market_hash_name, time DESC)
        """)

        await self.sqlite_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_history_timestamp
            ON price_history(time DESC)
        """)

        # Query the most recent timestamp we have for this item
        async with self.sqlite_conn.execute("""
            SELECT MAX(time) FROM price_history WHERE market_hash_name = ?
        """, (market_hash_name,)) as cursor:
            row = await cursor.fetchone()
            last_timestamp_str = row[0] if row else None

        # Parse last_timestamp from SQLite string format
        last_timestamp = None
        if last_timestamp_str:
            last_timestamp = datetime.strptime(last_timestamp_str, '%Y-%m-%d %H:%M:%S')

        # Extract currency from price_prefix or price_suffix
        currency = self._extract_currency(data.price_suffix) or self._extract_currency(data.price_prefix) or 'USD'

        # Steam returns data in ascending order (oldest → newest)
        # Iterate in reverse to find new points at the end, stop when we hit existing data
        records = []
        for price_point in reversed(data.prices):
            date_string, price, volume = price_point

            # Parse Steam's datetime format to proper timestamp
            parsed_time = self._parse_steam_datetime(date_string)
            if not parsed_time:
                continue  # Skip invalid dates

            # Stop when we reach data we already have
            if last_timestamp and parsed_time <= last_timestamp:
                break

            # Parse volume to integer
            volume_int = self._parse_volume(volume)
            if volume_int is None:
                volume_int = 0

            records.append((
                parsed_time.strftime('%Y-%m-%d %H:%M:%S'),  # SQLite datetime format
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
            return

        # Reverse to restore chronological order for insert
        records.reverse()

        # Batch insert with chunking
        BATCH_SIZE = 50  # SQLite performs best with smaller batches than PostgreSQL
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            await self.sqlite_conn.executemany("""
                INSERT INTO price_history (
                    time, appid, market_hash_name, item_nameid, currency, country, language, price, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_hash_name, time) DO NOTHING
            """, batch)

        await self.sqlite_conn.commit()

        print(f"  ✓ {market_hash_name}: {len(records)} new points")

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
            for symbol in ['$', '€', '£', '¥', '₽', 'pуб.', 'R$', 'CDN$', 'A$', 'HK$', 'S$', '₩', '₴', 'CHF', 'kr', 'zł', 'R', '฿']:
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
                return dt.replace(tzinfo=None)  # SQLite doesn't handle timezones well

            return None
        except (ValueError, IndexError, AttributeError):
            return None


# ============================================================================
# Async context manager usage example
# ============================================================================

async def example_usage():
    """Example of how schedulers will use DataWizard."""
    async with SQLinserts(
        sqlite_path="market_data.db",
        timescale_dsn="postgresql://user:pass@localhost/cs2market"  # Optional
    ) as wizard:
        # After fetching data from API:
        # result = await client.fetch_price_overview(...)
        # await wizard.store_data(result, item_config)
        pass
