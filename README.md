File Descriptions:

config.yaml: The Manifest. Entry point for user-defined parameters, asset lists, and target polling frequencies.

dataClasses.py: The Schema. Houses the Pydantic models that enforce data integrity and provide type-safety across the entire pipeline.

snoozerScheduler.py: The Real-Time Engine. A priority-based scheduler that calculates "urgency scores" for high-frequency assets (e.g., Live Price/Activity) and manages sub-minute execution loops.

clockworkScheduler.py: The Batch Engine. A time-synchronized scheduler designed for lower-frequency, high-volume tasks like hourly Price History snapshots.

rateLimiter.py: The Governor. A sliding-window algorithm that prevents API blacklisting by enforcing a strict global request budget (15 reqs/min).

utility_files/: The Localization Layer. Specialized parsers that handle international currency symbols, regional decimal formatting, and Unix timestamp conversions.

orchestrator.py: The Central Nervous System. The primary backend entry point; it initializes shared resources, injects the RateLimiter into schedulers, and manages the execution lifecycle.

SQLinserts.py: The Data Wizard. A routing layer that handles ACID-compliant transactions, batch-inserting parsed data into a hybrid SQLite/TimescaleDB storage system.

How to Run:

# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure your assets in config.yaml
# (Add your items, appIDs, and polling intervals)

# 3. Launch the system
python orchestrator.py
