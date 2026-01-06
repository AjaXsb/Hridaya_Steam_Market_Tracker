# Documentation

Technical reference for configuring, querying, and understanding the Steam Market data pipeline.

---

## Configuration Reference

All items are configured in `config.yaml` under `TRACKING_ITEMS`.

### Required Parameters (All Endpoints)

| Parameter | Type | Description |
|-----------|------|-------------|
| `market_hash_name` | string | Exact Steam market name (e.g., `"AK-47 \| Redline (Field-Tested)"`) |
| `appid` | integer | Steam application ID (see table below) |
| `apiid` | string | API endpoint to use (see API Endpoints section) |
| `polling-interval-in-seconds` | integer | How often to fetch data (minimum recommended: 8s) |

### Required Parameters (Endpoint-Specific)

| Parameter | Required For | Description |
|-----------|--------------|-------------|
| `item_nameid` | `itemordershistogram`, `itemordersactivity` | Numeric Steam item ID (find via browser dev tools on market page) |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `currency` | integer | `1` | Steam currency code (1=USD, 2=GBP, 3=EUR, etc.) |
| `country` | string | `"US"` | Two-letter country code |
| `language` | string | `"english"` | Language for API responses |

### Popular App IDs

| App ID | Game |
|--------|------|
| 730 | Counter-Strike 2 (CS2) |
| 570 | Dota 2 |
| 440 | Team Fortress 2 |
| 252490 | Rust |
| 753 | Steam (trading cards, backgrounds, emoticons) |

### Example Configuration

```yaml
LIMITS:
  REQUESTS: 15
  WINDOW_SECONDS: 60

TRACKING_ITEMS:
  - market_hash_name: "AK-47 | Redline (Field-Tested)"
    appid: 730
    currency: 1
    country: 'US'
    language: 'english'
    polling-interval-in-seconds: 30
    apiid: 'itemordershistogram'
```

---

## API Endpoints

Four endpoints are supported. Use these exact strings for the `apiid` field:

| apiid | Description | Auth Required | Frequency |
|-------|-------------|---------------|-----------|
| `priceoverview` | Current lowest price, median price, and 24h volume | No | Real-time (seconds) |
| `itemordershistogram` | Full order book (buy/sell orders at each price level) | No | Real-time (seconds) |
| `itemordersactivity` | Recent buy/sell activity feed | No | Real-time (seconds) |
| `pricehistory` | Historical hourly price and volume data | Yes (cookies) | Hourly |

### Authentication

The `pricehistory` endpoint requires Steam session cookies in your `.env` file:

```
sessionid=your_session_id
steamLoginSecure=your_steam_login_secure_token
```

These can be extracted from your browser's developer tools while logged into Steam.

---

## Database Schema

Data is stored in SQLite (`market_data.db`). Four tables capture different aspects of market data.

### Table: `price_overview`

**What it stores:** Snapshots of current market prices - the price you'd see if you looked at an item's listing page right now.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-incrementing row ID |
| `timestamp` | DATETIME | When this snapshot was taken |
| `appid` | INTEGER | Steam app ID (730 for CS2) |
| `market_hash_name` | TEXT | Item name |
| `item_nameid` | INTEGER | Steam's internal item ID (if available) |
| `currency` | TEXT | Currency code (USD, EUR, etc.) |
| `country` | TEXT | Country code used for request |
| `language` | TEXT | Language used for request |
| `lowest_price` | REAL | Cheapest current listing |
| `median_price` | REAL | Median sale price (recent) |
| `volume` | INTEGER | Number of sales in last 24 hours |

### Table: `orders_histogram`

**What it stores:** Order book snapshots - all the buy orders and sell orders at each price level, like a stock market depth chart.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-incrementing row ID |
| `timestamp` | DATETIME | When this snapshot was taken |
| `appid` | INTEGER | Steam app ID |
| `market_hash_name` | TEXT | Item name |
| `item_nameid` | INTEGER | Steam's internal item ID |
| `currency` | TEXT | Currency code |
| `country` | TEXT | Country code |
| `language` | TEXT | Language |
| `buy_order_table` | TEXT | JSON array of buy orders `[{price, quantity}, ...]` |
| `sell_order_table` | TEXT | JSON array of sell orders `[{price, quantity}, ...]` |
| `buy_order_graph` | TEXT | JSON graph data for visualization |
| `sell_order_graph` | TEXT | JSON graph data for visualization |
| `buy_order_count` | INTEGER | Total number of buy orders |
| `sell_order_count` | INTEGER | Total number of sell orders |
| `highest_buy_order` | REAL | Best bid price |
| `lowest_sell_order` | REAL | Best ask price |

### Table: `orders_activity`

**What it stores:** Recent trade activity - a feed of actual purchases and new listings as they happen.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-incrementing row ID |
| `timestamp` | DATETIME | When this snapshot was taken |
| `appid` | INTEGER | Steam app ID |
| `market_hash_name` | TEXT | Item name |
| `item_nameid` | INTEGER | Steam's internal item ID |
| `currency` | TEXT | Currency code |
| `country` | TEXT | Country code |
| `language` | TEXT | Language |
| `activity_raw` | TEXT | JSON array of raw HTML activity strings |
| `parsed_activities` | TEXT | JSON array of parsed activities `[{price, action, timestamp}, ...]` |
| `activity_count` | INTEGER | Number of activities in this snapshot |
| `steam_timestamp` | INTEGER | Unix timestamp from Steam's response |

### Table: `price_history`

**What it stores:** Historical price data - hourly aggregated prices and volumes going back years. Think of it like stock OHLC data.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-incrementing row ID |
| `time` | DATETIME | The hour this data point represents |
| `appid` | INTEGER | Steam app ID |
| `market_hash_name` | TEXT | Item name |
| `item_nameid` | INTEGER | Steam's internal item ID (if available) |
| `currency` | TEXT | Currency code |
| `country` | TEXT | Country code |
| `language` | TEXT | Language |
| `price` | REAL | Median price during this hour |
| `volume` | INTEGER | Number of sales during this hour |
| `fetched_at` | DATETIME | When we fetched this data |

---

## Querying the Database

Connect to the database:

```bash
sqlite3 market_data.db
```

### Basic Queries

**Get the latest price for an item:**
```sql
SELECT timestamp, lowest_price, median_price, volume
FROM price_overview
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
ORDER BY timestamp DESC
LIMIT 1;
```

**Get all prices from the last hour:**
```sql
SELECT timestamp, lowest_price, median_price
FROM price_overview
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
  AND timestamp > datetime('now', '-1 hour')
ORDER BY timestamp DESC;
```

**Get the current bid-ask spread:**
```sql
SELECT timestamp, highest_buy_order, lowest_sell_order,
       (lowest_sell_order - highest_buy_order) AS spread
FROM orders_histogram
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
ORDER BY timestamp DESC
LIMIT 1;
```

### Historical Analysis

**Daily average price over the last 30 days:**
```sql
SELECT date(time) AS day,
       AVG(price) AS avg_price,
       SUM(volume) AS total_volume
FROM price_history
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
  AND time > datetime('now', '-30 days')
GROUP BY date(time)
ORDER BY day DESC;
```

**Hourly price trend for today:**
```sql
SELECT strftime('%H:00', time) AS hour, price, volume
FROM price_history
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
  AND date(time) = date('now')
ORDER BY time;
```

**Find price spikes (prices 20% above average):**
```sql
WITH avg_price AS (
    SELECT AVG(price) AS mean FROM price_history
    WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
)
SELECT time, price, volume
FROM price_history, avg_price
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
  AND price > mean * 1.2
ORDER BY time DESC;
```

### Order Book Analysis

**Get full order book (latest snapshot):**
```sql
SELECT timestamp, buy_order_table, sell_order_table
FROM orders_histogram
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
ORDER BY timestamp DESC
LIMIT 1;
```

**Track buy order count over time:**
```sql
SELECT timestamp, buy_order_count, sell_order_count
FROM orders_histogram
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
  AND timestamp > datetime('now', '-24 hours')
ORDER BY timestamp;
```

### Activity Analysis

**Get recent trades:**
```sql
SELECT timestamp, parsed_activities, activity_count
FROM orders_activity
WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
ORDER BY timestamp DESC
LIMIT 10;
```

### Cross-Item Comparisons

**Compare current prices across all tracked items:**
```sql
SELECT market_hash_name,
       MAX(timestamp) AS last_update,
       lowest_price,
       volume
FROM price_overview
GROUP BY market_hash_name
ORDER BY volume DESC;
```

**Most volatile items (by price range in last 24h):**
```sql
SELECT market_hash_name,
       MIN(price) AS low,
       MAX(price) AS high,
       MAX(price) - MIN(price) AS range,
       (MAX(price) - MIN(price)) / AVG(price) * 100 AS volatility_pct
FROM price_history
WHERE time > datetime('now', '-24 hours')
GROUP BY market_hash_name
ORDER BY volatility_pct DESC;
```

### Useful Tips

1. **JSON data:** Use `json_extract()` to query inside JSON columns:
   ```sql
   SELECT json_extract(buy_order_table, '$[0].price') AS top_bid
   FROM orders_histogram
   WHERE market_hash_name = 'AK-47 | Redline (Field-Tested)'
   ORDER BY timestamp DESC LIMIT 1;
   ```

2. **Export to CSV:**
   ```bash
   sqlite3 -header -csv market_data.db "SELECT * FROM price_history" > export.csv
   ```

3. **Time zones:** All timestamps are stored in UTC.

4. **Performance:** Tables are indexed on `(market_hash_name, timestamp DESC)` for fast lookups.