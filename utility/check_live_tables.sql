-- Live-table correctness checks. Run:
--   podman exec -i cs2-timescale psql -U postgres -d cs2market -f - < utility/check_live_tables.sql

-- 1. counts / span / currencies per table
SELECT 'price_overview' t, count(*), array_agg(DISTINCT currency) cur, max(timestamp) newest FROM price_overview
UNION ALL SELECT 'orders_histogram', count(*), array_agg(DISTINCT currency), max(timestamp) FROM orders_histogram
UNION ALL SELECT 'orders_activity', count(*), array_agg(DISTINCT currency), max(timestamp) FROM orders_activity;

-- 2. overview sanity: flag non-positive or lowest>median (per currency)
SELECT market_hash_name, currency, lowest_price, median_price
FROM price_overview
WHERE lowest_price IS NULL OR lowest_price <= 0 OR lowest_price > median_price;

-- 3. histogram: crossed book or count/array mismatch (note: scalars are CENTS)
SELECT market_hash_name, currency, highest_buy_order, lowest_sell_order,
       (lowest_sell_order > highest_buy_order) AS spread_ok,
       buy_order_count, jsonb_array_length(buy_order_table) AS buy_levels,
       sell_order_count
FROM orders_histogram ORDER BY timestamp DESC;

-- 4. activity: count must equal both JSONB array lengths
SELECT market_hash_name, activity_count,
       jsonb_array_length(parsed_activities) AS parsed_len,
       jsonb_array_length(activity_raw) AS raw_len,
       (activity_count = jsonb_array_length(parsed_activities)
        AND activity_count = jsonb_array_length(activity_raw)) AS counts_match
FROM orders_activity ORDER BY timestamp DESC;
