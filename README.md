# Hridaya (हृदय)

**The Heart of Market Ingestion**

Hridaya is a professional-grade asynchronous data engine engineered to overcome the unique challenges of the Steam Community Market. Unlike standard scrapers, Hridaya acts as a central nervous system for market data, strategically timing every pulse of the exchange to maintain a high-fidelity record of market width and depth.

## Key Technical Achievements

* **Strategic Ingestion:** Intelligent scheduling that balances real-time order-book snapshots with massive historical data fetches.
* **Resource Sovereignty:** A global orchestrator managing a shared sliding-window rate-limit budget across concurrent asynchronous tasks.
* **Financial Grade Integrity:** Automated data sanitization translating regional currency symbols and non-standard timestamps into structured numeric "Tape."

## System Architecture

### **Cerebro (The Brain)**
**`cerebro.py`**: The primary entry point. It acts as the central consciousness of the engine, initializing shared resources, injecting the `RateLimiter` into schedulers, and managing the global execution lifecycle.

### **The Internal Machinery (`/src`)**
* **`rateLimiter.py`**: **The Governor.** A thread-safe, sliding-window algorithm that prevents API blacklisting by enforcing a strict global request budget defined in the configuration.
* **`snoozerScheduler.py`**: **The Real-Time Engine.** A priority-based scheduler calculating "urgency scores" for high-frequency assets to manage sub-minute execution loops.
* **`clockworkScheduler.py`**: **The Batch Engine.** A time-synchronized scheduler designed for high-volume, lower-frequency tasks like hourly price history snapshots.
* **`dataClasses.py`**: **The Schema.** Houses Pydantic models that enforce data integrity and provide type-safety across the entire pipeline.
* **`SQLinserts.py`**: **The Data Wizard.** A routing layer handling ACID-compliant transactions and batch-inserting data into the storage layer.

### **The Localization Layer (`/utility`)**
* **`loadConfig_utility.py`**: Handles manifest ingestion and environment setup.
* **`parseActivityHTML_utility.py`**: Specialized parsers handling international currency symbols and regional decimal formatting.

## How to Run

1.  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure assets in `config.yaml`**
    Add your items, appIDs, and polling intervals. Create a `.env` file with `sessionid` and `steamLoginSecure` cookies for price history access.

3.  **Launch the system**
    ```bash
    python cerebro.py
    ```

## Collaboration & Contribution

Fork it, clone it, try it, modify it, create pull requests if you find improvements and lets collaborate on the frontend, AI integration or a thousand others things that can be done on this datamine of gold.

Currently, only cs2 items are supported as hridaya only has access to cs2 item name ids. check loadConfig_utility file for implementation.

Special thanks to [Revadike](https://github.com/Revadike/InternalSteamWebAPI) and [somespecialone](https://github.com/somespecialone/steam-item-name-ids) for their foundational work on the Steam Web API and item name IDs.

**GLHF!**