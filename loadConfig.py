import yaml
import json
import urllib.request
from pathlib import Path
from typing import Dict


def fetch_cs2_item_name_ids() -> Dict[str, int]:
    """
    Fetch CS2 item name IDs from GitHub repository.

    Returns:
        Dictionary mapping market_hash_name to item_nameid
    """
    cache_file = Path(".serena/cs2_item_ids.json")

    # Use cached version if it exists
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)

    # Fetch from GitHub
    url = "https://raw.githubusercontent.com/somespecialone/steam-item-name-ids/master/data/cs2.json"

    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())

        # Cache it locally
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)

        return data

    except Exception as e:
        print(f"Warning: Could not fetch item name IDs from GitHub: {e}")
        return {}


def populate_item_name_ids(config: dict) -> dict:
    """
    Populate item_nameid field for items in config that are missing it.

    Args:
        config: Configuration dictionary

    Returns:
        Updated configuration dictionary
    """
    item_id_map = fetch_cs2_item_name_ids()

    for item in config.get('TRACKING_ITEMS', []):
        # Only populate if item_nameid is missing and we need it (histogram or activity)
        apiid = item.get('apiid')
        if 'item_nameid' not in item and apiid in ['itemordershistogram', 'itemordersactivity']:
            market_hash_name = item.get('market_hash_name')

            if market_hash_name in item_id_map:
                item['item_nameid'] = item_id_map[market_hash_name]
                print(f"  ✓ Found item_nameid for '{market_hash_name}': {item['item_nameid']}")
            else:
                print(f"  ✗ Warning: No item_nameid found for '{market_hash_name}'")

    return config


def load_config_from_yaml(config_path: str = "config.yaml") -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to the YAML configuration file (default: "config.yaml")

    Returns:
        Dictionary containing the configuration data
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    # Auto-populate item_nameid fields
    config = populate_item_name_ids(config)

    return config


# Example usage
if __name__ == "__main__":
    # Load the config
    config = load_config_from_yaml()

    # Access the values
    print("Rate Limits:")
    print(f"  Requests: {config['LIMITS']['REQUESTS']}")
    print(f"  Window: {config['LIMITS']['WINDOW_SECONDS']} seconds")

    print("\nTracking Items:")
    for item in config['TRACKING_ITEMS']:
        print(f"  - {item['market_hash_name']} ({item['apiid']})") 
