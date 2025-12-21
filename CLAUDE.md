# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based Steam Market API client for tracking CS2 (Counter-Strike 2) item prices and market data. The application uses async/await patterns with aiohttp for concurrent API requests and implements a sliding window rate limiter to respect Steam's API limits (15 requests per 60 seconds).

## Environment Setup

1. **Python Version**: Python 3.13+
2. **Virtual Environment**: Project uses venv in the `venv/` directory
3. **Dependencies**: Install via `pip install -r requirements.txt`
   - aiohttp (async HTTP client)
   - python-dotenv (environment variable management)
   - pydantic (data validation)
   - pyyaml (configuration parsing)

4. **Environment Variables**: Create a `.env` file in the root directory with:
   ```
   STEAM_SESSION_ID=your_session_id
   STEAM_LOGIN_SECURE=your_login_secure_token
   ```
   These cookies are required for authenticated Steam Market API access.

## Running the Application

- **Main entry point**: `python main.py` (note: main.py appears to be minimal/incomplete in current state)
- **Test config loading**: `python load_config_from_yaml.py`
- **Virtual environment**: Activate with `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)

## Architecture

### Core Components

1. **SteamAPIClient** (`SteamAPIClient.py`)
   - Async HTTP client wrapper for Steam Market API
   - Implements four API endpoints:
     - `fetch_price_overview()`: Current lowest/median prices and volume
     - `fetch_orders_histogram()`: Order book data (buy/sell orders)
     - `fetch_orders_activity()`: Recent trade activity
     - `fetch_price_history()`: Historical OHLCV data
   - Automatically injects Steam authentication cookies from environment
   - Every API method calls `await self.rate_limiter.acquire_token()` before making requests
   - Supports async context manager protocol (`async with`)

2. **RateLimiter** (`RateLimiter.py`)
   - Implements Sliding Window Log algorithm
   - Enforces 15 requests per 60-second window
   - Thread-safe using `asyncio.Lock`
   - Automatically waits when rate limit is reached
   - Critical for preventing Steam API bans

3. **Data Models** (`data_classes.py`)
   - All models use Pydantic BaseModel for validation
   - Configuration models: `ItemConfig`, `SchedulerConfig`, `ScheduledTask`
   - API response models: `PriceOverviewData`, `OrdersHistogramData`, `OrdersActivityData`, `PriceHistoryData`
   - Nested models: `OrderBookEntry`, `ActivityEntry`, `PriceHistoryPoint`
   - Note: Configuration models reference a scheduler system (T_live, last_live_update, etc.) that may not be fully implemented yet

4. **Configuration** (`load_config_from_yaml.py`, `config.yaml`)
   - YAML-based configuration system
   - `LIMITS`: Rate limiter settings (REQUESTS, WINDOW_SECONDS)
   - `TRACKING_ITEMS`: List of items to track with their API endpoints
   - Each item has: market_hash_name, appid, currency, country, language, latency-in-seconds, apiid
   - Supports multiple API endpoints per item (pricehistory, priceoverview, itemordershistogram, itemordersactivity)

### Data Flow

1. Configuration is loaded from `config.yaml`
2. Steam API client is initialized with session cookies from `.env`
3. Rate limiter ensures all API calls respect Steam's limits
4. API responses are returned as raw JSON dictionaries (Pydantic models exist but may not be actively used for parsing)

## Development Guidelines

### Rate Limiting

The rate limiter is CRITICAL. Every Steam API call MUST go through `await self.rate_limiter.acquire_token()` before execution. Never bypass this mechanism or you risk getting the IP banned from Steam's API.

### Async Patterns

All API operations are asynchronous. When adding new functionality:
- Use `async def` for coroutines
- Always `await` async operations
- Use `async with` for the SteamAPIClient context manager
- Use `asyncio.sleep()` instead of `time.sleep()`

### Configuration Changes

When modifying tracked items or rate limits, edit `config.yaml`. The structure is:
- `LIMITS.REQUESTS`: Max requests in window
- `LIMITS.WINDOW_SECONDS`: Time window in seconds
- `TRACKING_ITEMS`: Array of item configurations

### Steam API Specifics

- `appid`: 730 is CS2, other Steam games have different IDs
- `market_hash_name`: URL-encoded item name (e.g., "AK-47 | Redline (Field-Tested)")
- `item_nameid`: Numeric ID required for histogram endpoint (find via browser inspection)
- Endpoints return different data structures - refer to Pydantic models in `data_classes.py` for expected schemas

### Function Naming Convention

Functions should be named as descriptions of what they do (e.g., `fetch_price_overview`, `acquire_token`, `load_config_from_yaml`).
