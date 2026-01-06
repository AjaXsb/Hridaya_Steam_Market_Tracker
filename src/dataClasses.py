"""
Pydantic models for Steam Market API response data contracts.

Contains response models matching Steam's Market API endpoints.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# ============================================================================
# Steam API Response Models
# ============================================================================


class PriceOverviewData(BaseModel):
    """
    Response model for /priceoverview endpoint.

    Attributes:
        success: Whether the API call succeeded
        lowest_price: Current lowest listing price (formatted string)
        median_price: Recent median sale price (formatted string)
        volume: Recent sales volume (formatted string)
    """

    success: bool
    lowest_price: Optional[str] = None
    median_price: Optional[str] = None
    volume: Optional[str] = None


class OrderBookEntry(BaseModel):
    """
    Nested model representing a single order book entry (buy or sell).

    Attributes:
        price: Price as formatted string
        quantity: Number of orders at this price level
    """

    price: str
    quantity: str  # API returns string, not int


class OrdersHistogramData(BaseModel):
    """
    Response model for /itemordershistogram endpoint.

    Attributes:
        success: Whether the API call succeeded (Steam returns 1 for true)
        sell_order_count: Total number of sell orders
        sell_order_price: Lowest sell order price
        sell_order_table: List of sell order book entries
        buy_order_count: Total number of buy orders
        buy_order_price: Highest buy order price
        buy_order_table: List of buy order book entries
        highest_buy_order: Highest buy order price string
        lowest_sell_order: Lowest sell order price string
        buy_order_graph: Graph data for buy orders
        sell_order_graph: Graph data for sell orders
        graph_max_y: Max Y value for graph
        graph_min_x: Min X value for graph
        graph_max_x: Max X value for graph
        price_prefix: Currency prefix
        price_suffix: Currency suffix
    """

    success: int | bool  # Steam returns 1 for success
    sell_order_count: int | str | None = None
    sell_order_price: Optional[str] = None
    sell_order_table: Optional[List[OrderBookEntry]] = None
    buy_order_count: int | str | None = None
    buy_order_price: Optional[str] = None
    buy_order_table: Optional[List[OrderBookEntry]] = None
    highest_buy_order: Optional[str] = None
    lowest_sell_order: Optional[str] = None
    buy_order_graph: List = Field(default_factory=list)
    sell_order_graph: List = Field(default_factory=list)
    graph_max_y: int | None = None
    graph_min_x: float | None = None
    graph_max_x: float | None = None
    price_prefix: str = ""
    price_suffix: str = ""


class ActivityEntry(BaseModel):
    """
    Parsed model representing a single trade activity event.

    This is NOT returned by the API - it's created by parsing the HTML strings
    in the activity array.

    Attributes:
        price: Trade price as string (e.g., "0.85")
        currency: ISO currency code (e.g., "EUR", "USD")
        action: Activity action (e.g., "Purchased", "Listed")
        timestamp: When the activity occurred
        raw_html: Original HTML string
    """

    price: Optional[str] = None
    currency: Optional[str] = None
    action: Optional[str] = None
    timestamp: Optional[datetime] = None
    raw_html: str


class OrdersActivityData(BaseModel):
    """
    Response model for /itemordersactivity endpoint.

    Attributes:
        success: Whether the API call succeeded (Steam returns 1 for true)
        activity: List of HTML strings containing trade activity
        timestamp: Unix timestamp of the response
        parsed_activities: Optional parsed activity entries (added by parser)
    """

    success: int | bool  # Steam returns 1 for success
    activity: List[str] = Field(default_factory=list)  # Raw HTML strings
    timestamp: int
    parsed_activities: Optional[List[ActivityEntry]] = None  # Added after parsing


class PriceHistoryPoint(BaseModel):
    """
    Parsed model representing a single price history data point.

    This is NOT returned by the API - it's created by parsing the arrays
    in the prices field.

    Attributes:
        date_string: Date as string (e.g., "Jul 02 2014 01: +0")
        price: Median price at this time
        volume: Trading volume (as string from API)
    """
    date_string: str
    price: float
    volume: str  # API returns volume as string


class PriceHistoryData(BaseModel):
    """
    Response model for /pricehistory endpoint.

    Attributes:
        success: Whether the API call succeeded
        price_prefix: Currency prefix (e.g., "$")
        price_suffix: Currency suffix (e.g., "€")
        prices: List of [date_string, price_float, volume_string] arrays
    """

    success: bool
    price_prefix: str = ""
    price_suffix: str = ""
    prices: List[List] = Field(default_factory=list)  # Array of [date_str, price_float, volume_str]
