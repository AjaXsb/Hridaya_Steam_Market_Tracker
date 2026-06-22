"""Read-only FastAPI read-path API for the CS2 market data store.

Serves the frontend from the same Postgres/Timescale instance that the
ingestion schedulers write to. This package never writes to the database.
"""
