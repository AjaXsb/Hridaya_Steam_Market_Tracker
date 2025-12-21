"""
Parser for Steam Market activity HTML responses.

Extracts structured data from HTML-formatted activity entries.
"""

import re
from typing import Optional, Dict
from datetime import datetime


def parse_price_and_currency(price_str: str) -> tuple[Optional[float], Optional[str]]:
    """
    Parse price string with currency symbol into numeric value and currency code.

    Examples:
        "0,85€" -> (0.85, "EUR")
        "$12.50" -> (12.50, "USD")
        "£5.99" -> (5.99, "GBP")

    Args:
        price_str: Price string with currency symbol

    Returns:
        Tuple of (price_value, currency_code)
    """
    price_str = price_str.strip()

    # Currency symbol mapping
    currency_map = {
        '€': 'EUR',
        '$': 'USD',
        '£': 'GBP',
        '¥': 'JPY',
        '₽': 'RUB',
        'R$': 'BRL',
        'CDN$': 'CAD',
        'A$': 'AUD',
    }

    # Extract currency symbol
    currency = None
    for symbol, code in currency_map.items():
        if symbol in price_str:
            currency = code
            price_str = price_str.replace(symbol, '').strip()
            break

    if not price_str:
        return None, None

    # Handle European format (comma as decimal separator)
    price_str = price_str.replace(',', '.')

    # Extract numeric value
    try:
        price = float(price_str)
        return price, currency
    except ValueError:
        return None, None


def parse_activity_html(html: str) -> Dict:
    """
    Parse Steam market activity HTML into structured data.

    Args:
        html: HTML string from Steam activity response

    Returns:
        Dictionary with parsed fields: price, currency, action, raw_html
    """
    # Extract price (from span with class market_activity_price)
    price_pattern = r'<span class="market_activity_cell market_activity_price[^"]*">\s*([^<]+?)\s*</span>'
    price_matches = re.findall(price_pattern, html)

    # Find the non-empty price (Steam returns multiple price spans, usually middle one has data)
    price = None
    currency = None
    for price_str in price_matches:
        price_str = price_str.strip()
        if price_str:
            price, currency = parse_price_and_currency(price_str)
            if price is not None:
                break

    # Extract action (Purchased, Listed, etc.)
    action_pattern = r'<span class="market_activity_action">([^<]+)</span>'
    action_match = re.search(action_pattern, html)
    action = action_match.group(1).strip() if action_match else None

    return {
        "price": price,
        "currency": currency,
        "action": action,
        "raw_html": html
    }


def parse_activity_response(response: dict) -> list[dict]:
    """
    Parse full Steam activity response into list of structured entries.

    Args:
        response: Full response from fetch_orders_activity

    Returns:
        List of parsed activity entries with timestamp
    """
    if not response.get("success"):
        return []

    timestamp = response.get("timestamp")
    parsed_entries = []

    for html_line in response.get("activity", []):
        parsed = parse_activity_html(html_line)
        parsed["timestamp"] = datetime.fromtimestamp(timestamp) if timestamp else None
        parsed_entries.append(parsed)

    return parsed_entries


# Test function
def test_parse_activity():
    """Test the parser with example Steam activity data"""

    # Example response from Steam
    example_response = {
        "success": 1,
        "activity": [
            '<div class="market_activity_line_item ellipsis">\n\t<span class="market_activity_placeholder"></span>\n\t<span class="market_activity_cell market_activity_price ">\n\t\t\t</span>\n\t<span class="market_activity_cell market_activity_price ">\n\t\t0,85€\t</span>\n\t<span class="market_activity_cell market_activity_price ">\n\t\t\t</span>\n\t<span class="market_activity_action">Purchased</span>\n</div>\n',
            '<div class="market_activity_line_item ellipsis">\n\t<span class="market_activity_placeholder"></span>\n\t<span class="market_activity_cell market_activity_price ">\n\t\t\t</span>\n\t<span class="market_activity_cell market_activity_price ">\n\t\t$12.50\t</span>\n\t<span class="market_activity_cell market_activity_price ">\n\t\t\t</span>\n\t<span class="market_activity_action">Listed</span>\n</div>\n'
        ],
        "timestamp": 1765687694
    }

    print("Testing Steam Activity Parser")
    print("=" * 60)

    parsed = parse_activity_response(example_response)

    for i, entry in enumerate(parsed, 1):
        print(f"\nEntry {i}:")
        print(f"  Price: {entry['price']}")
        print(f"  Currency: {entry['currency']}")
        print(f"  Action: {entry['action']}")
        print(f"  Timestamp: {entry['timestamp']}")

    print("\n" + "=" * 60)
    print(f"✓ Parsed {len(parsed)} entries successfully")

    return parsed


if __name__ == "__main__":
    test_parse_activity()
