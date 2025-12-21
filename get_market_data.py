import requests
import json
import time

# --- Your Credentials and Item ---
# Replace these with the cookies from your logged-in Steam browser session.
#sessionid:"28a812d0788825fbb0a38051"
# steamLoginSecure:"76561198324763607%7C%7CeyAidHlwIjogIkpXVCIsICJhbGciOiAiRWREU0EiIH0.eyAiaXNzIjogInI6MDAxNF8yNjU2QTU5NV9CRTQzNyIsICJzdWIiOiAiNzY1NjExOTgzMjQ3NjM2MDciLCAiYXVkIjogWyAid2ViOmNvbW11bml0eSIgXSwgImV4cCI6IDE3NTc4MTU1NTMsICJuYmYiOiAxNzQ5MDg4NjQxLCAiaWF0IjogMTc1NzcyODY0MSwgImp0aSI6ICIwMDA1XzI2RUNGMjlCX0JCQUQ3IiwgIm9hdCI6IDE3NDgzMTI2NTcsICJydF9leHAiOiAxNzY2MjU4MjEzLCAicGVyIjogMCwgImlwX3N1YmplY3QiOiAiNzEuMTc0LjIzOC4xNjEiLCAiaXBfY29uZmlybWVyIjogIjcxLjE3NC4yMzguMTYxIiB9.c_GHEi0Bs00ngSDsfR7Pn_DmxhiYnoC-gZxXqc82DKl-LxDZjXJhIgl4Unxe00_mstwEXQmunq11EbUYLHitBw"
steam_cookies = {
    'sessionid': 'aeb368598bdb276f4082670e',
    "steamLoginSecure": "776561198324763607%7C%7CeyAidHlwIjogIkpXVCIsICJhbGciOiAiRWREU0EiIH0.eyAiaXNzIjogInI6MDAwQV8yNzYzOTQzNV9DNUNBNSIsICJzdWIiOiAiNzY1NjExOTgzMjQ3NjM2MDciLCAiYXVkIjogWyAid2ViOmNvbW11bml0eSIgXSwgImV4cCI6IDE3NjU5MjAyODEsICJuYmYiOiAxNzU3MTkyNjM1LCAiaWF0IjogMTc2NTgzMjYzNSwgImp0aSI6ICIwMDA1XzI3NjM5MzQ3XzE4ODc0IiwgIm9hdCI6IDE3NjU2ODc1MzYsICJydF9leHAiOiAxNzgzODY0MDgwLCAicGVyIjogMCwgImlwX3N1YmplY3QiOiAiOTYuMjUyLjEwNi4xNDMiLCAiaXBfY29uZmlybWVyIjogIjk2LjI1Mi4xMDYuMTQzIiB9.qnTJkn8iKOa8I9_dGKDV9mT_CmHXoqE3IwBtcoumhlY-AJGVQ30JX2LEdvZpPXFvBQmz7oRPtMktxIkURj2tDA"
}

# The App ID for the game.
app_id = 730  # Counter-Strike 2

# The URL-encoded name of the item you want to analyze.
# Example: 'StatTrak™ AWP | Asiimov (Field-Tested)'
market_hash_name = 'StatTrak%E2%84%A2%20AWP%20%7C%20Asiimov%20%28Field-Tested%29'

# --- API Request ---
url = f'https://steamcommunity.com/market/pricehistory/?appid={app_id}&market_hash_name={market_hash_name}'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Referer': f'https://steamcommunity.com/market/listings/{app_id}/{market_hash_name}'
}

try:
    response = requests.get(url, headers=headers, cookies=steam_cookies)
    response.raise_for_status()

    data = response.json()
    
    if data['success']:
        filename = f"{market_hash_name.replace('%20', '_').replace('|', '-').replace('™', 'TM')}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"✅ Successfully fetched and saved data for '{market_hash_name}' to {filename}.")
    else:
        print("❌ API request was not successful. Check your cookies and the item name.")

except requests.exceptions.RequestException as e:
    print(f"❌ Error during API request: {e}")

# This is a must. Do not run this without a delay.
time.sleep(3)