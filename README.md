# Hridaya (हृदय)

The Heart of Market Ingestion

Hridaya is an asynchronous data engine built to overcome the unique challenges of the Steam Community Market. Unlike standard scrapers, Hridaya acts as a central nervous system for market data, strategically timing every pulse of the exchange to maintain a 1:1 ratio between the width of the market and the depth of its historical records.

Key Technical Achievements:

* Strategic Ingestion: Intelligent scheduling that balances immediate order-book snapshots with massive historical data fetches.

* Resource Sovereignty: A global orchestrator that manages a single shared rate-limit budget across multiple concurrent schedulers.

* Financial Grade Integrity: Automated data sanitization that translates regional currency symbols and non-standard timestamps into clean, numeric financial "Tape."

## File Descriptions:

***config.yaml***: The Manifest. Entry point to track items, api endpoints, and target polling frequencies.

***dataClasses.py***: The Schema. Houses the Pydantic models that enforce data integrity and provide type-safety across the entire pipeline.

***snoozerScheduler.py***: The Real-Time Engine. A priority-based scheduler that calculates "urgency scores" for high-frequency assets (e.g., Live Price/Activity) and manages sub-minute execution loops.

***clockworkScheduler.py***: The Batch Engine. A time-synchronized scheduler designed for lower-frequency, high-volume tasks like hourly Price History snapshots.

***rateLimiter.py***: The Governor. A sliding-window algorithm that prevents API blacklisting by enforcing a strict global request budget stated in the config.

***_utility files***: The Localization Layer. Specialized parsers that handle international currency symbols, regional decimal formatting, and Unix timestamp conversions.

***orchestrator.py***: The Central Nervous System. The primary backend entry point; it initializes shared resources, injects the RateLimiter into schedulers, and manages the execution lifecycle.

***SQLinserts.py***: The Data Wizard. A routing layer that handles ACID-compliant transactions, batch-inserting parsed data into a hybrid SQLite/TimescaleDB storage system.

## How to Run:

# 1. Install dependencies
`pip install -r requirements.txt`

# 2. Configure your assets in config.yaml
Add your items, appIDs, and polling intervals
Don't forget to create a .env file with the sessionid and steamLoginSecure cookie values if you are going to call the price history endpoint.

# 3. Launch the system
`python orchestrator.py`

### A note to everyone:

Fork it, clone it, try it, modify it, create pull requests if you find improvements and lets collaborate on the frontend, AI integration or a thousand others things that can be done on this datamine of gold.

Currently, only cs2 items are supported as hridaya only has access to cs2 item name ids. check loadConfig_utility file for implementation.

Special thanks to [Revadike](https://github.com/Revadike/InternalSteamWebAPI) and [somespecialone](https://github.com/somespecialone/steam-item-name-ids) for their work on the steam api and steam item name ids.

GLHF!