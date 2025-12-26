import os
import json
import aiohttp
from dotenv import load_dotenv
from RateLimiter import RateLimiter
from dataClasses import (
    PriceOverviewData,
    OrdersHistogramData,
    OrdersActivityData,
    PriceHistoryData
)
from parseActivityHTML import parse_activity_response

# Load environment variables from .env file
load_dotenv()


class SteamAPIClient:
    """
    Async client for Steam Market API with built-in rate limiting.

    Enforces 14 requests per 60-second window to safely stay within
    Steam's 15 requests/60s rate limit.
    """

    BASE_URL = "https://steamcommunity.com/market/"

    def __init__(self):
        """
        Initialize the Steam API client.

        Args:
            api_key: Optional Steam Web API key for authenticated requests
        """
        self.rate_limiter = RateLimiter()
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the aiohttp session. Call this when done with the client."""
        await self.session.close()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def fetch_price_overview(self, appid: int, market_hash_name: str) -> PriceOverviewData:
        """
        Fetch price overview for a specific item.

        Args:
            appid: Steam application ID (e.g., 730 for CS2)
            market_hash_name: URL-encoded market hash name of the item

        Returns:
            Parsed PriceOverviewData object
        """
        # CRITICAL: Acquire rate limit token before making request
        await self.rate_limiter.acquire_token()

        url = f"{self.BASE_URL}priceoverview/"
        params = {
            "appid": appid,
            "market_hash_name": market_hash_name
        }

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            raw_response = await response.json()
            # Parse into Pydantic model - validation errors will bubble up to scheduler
            return PriceOverviewData(**raw_response)

    async def fetch_orders_histogram(
        self, appid: int, item_nameid: int, currency, country: str = "US", language: str = "english"
    ) -> OrdersHistogramData:
        """
        Fetch order book histogram (buy/sell orders) for a specific item.

        Args:
            appid: Steam application ID (e.g., 730 for CS2)
            item_nameid: Numeric Steam item name ID
            currency: Currency code (default 1 for USD)
            country: Country code (default "US")
            language: Language (default "english")

        Returns:
            Parsed OrdersHistogramData object
        """
        # CRITICAL: Acquire rate limit token before making request
        await self.rate_limiter.acquire_token()

        url = f"{self.BASE_URL}itemordershistogram"
        params = {
            "norender": 1,
            "appid": appid,
            "item_nameid": item_nameid,
            "currency": currency,
            "country": country,
            "language": language
        }

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            raw_response = await response.json()
            # Parse into Pydantic model - validation errors will bubble up to scheduler
            return OrdersHistogramData(**raw_response)

    async def fetch_orders_activity(
        self, item_nameid: int, country: str = "US", language: str = "english",
        currency: int = 1, two_factor: int = 0
    ) -> OrdersActivityData:
        """
        Fetch recent orders activity for a specific item.

        Note: This endpoint returns JSON with HTML content in the "activity" field.
        The HTML is automatically parsed into structured ActivityEntry objects.

        Args:
            item_nameid: Numeric Steam item name ID
            country: Country code (default "US")
            language: Language (default "english")
            currency: Currency code (default 1 for USD)
            two_factor: Two factor flag (default 0)

        Returns:
            Parsed OrdersActivityData object with parsed_activities populated
        """
        # CRITICAL: Acquire rate limit token before making request
        await self.rate_limiter.acquire_token()

        url = f"{self.BASE_URL}itemordersactivity"
        params = {
            "country": country,
            "language": language,
            "currency": currency,
            "item_nameid": item_nameid,
            "two_factor": two_factor
        }

        async with self.session.get(url, params=params) as response:
            response.raise_for_status()
            # Steam returns JSON but with text/html Content-Type, so parse manually
            text = await response.text()
            raw_response = json.loads(text)

            # Parse into Pydantic model
            data = OrdersActivityData(**raw_response)

            # Parse HTML activity strings into structured data
            data.parsed_activities = parse_activity_response(raw_response)

            return data

    async def fetch_price_history(self, appid: int, market_hash_name: str) -> PriceHistoryData:
        """
        Fetch historical price data for a specific item.

        Args:
            appid: Steam application ID (e.g., 730 for CS2)
            market_hash_name: URL-encoded market hash name of the item

        Returns:
            Parsed PriceHistoryData object
        """
        # CRITICAL: Acquire rate limit token before making request
        await self.rate_limiter.acquire_token()

        # Load authentication cookies from environment (required for pricehistory)
        session_id = os.getenv("sessionid")
        steam_login_secure = os.getenv("steamLoginSecure")
        browser_id = os.getenv("browserid")
        steam_country = os.getenv("steamCountry")

        cookies = {}
        if session_id:
            cookies["sessionid"] = session_id
        if steam_login_secure:
            cookies["steamLoginSecure"] = steam_login_secure
        if browser_id:
            cookies["browserid"] = browser_id
        if steam_country:
            cookies["steamCountry"] = steam_country

        url = f"{self.BASE_URL}pricehistory"
        params = {
            "appid": appid,
            "market_hash_name": market_hash_name
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Referer': f'https://steamcommunity.com/market/listings/{appid}/{market_hash_name}'
        }

        async with self.session.get(url, params=params, cookies=cookies, headers=headers) as response:
            response.raise_for_status()
            raw_response = await response.json()
            # Parse into Pydantic model - validation errors will bubble up to scheduler
            return PriceHistoryData(**raw_response)
