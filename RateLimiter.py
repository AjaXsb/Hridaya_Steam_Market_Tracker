import asyncio
import time


class RateLimiter:
    """
    Rate limiter using Sliding Window Log algorithm.
    Enforces a limit of 14 requests per 60-second window for Steam Web API safety.
    """

    def __init__(self):
        """Initialize the rate limiter with an empty timestamp log and asyncio lock."""
        self._lock = asyncio.Lock()
        self._timestamps: list[float] = []

    async def acquire_token(self) -> None:
        """
        Acquire a token to make a request, waiting if necessary to respect rate limits.

        This method ensures that no more than 15 requests occur within any 60-second window.
        If the limit is reached, it waits until a slot becomes available.
        """
        while True:
            async with self._lock:
                current_time = time.time()
                cutoff_time = current_time - 60.0
                self._timestamps = [ts for ts in self._timestamps if ts > cutoff_time]

                # Check if we've hit the rate limit
                if len(self._timestamps) >= 15:
                    # Calculate exact wait time until oldest timestamp is 60 seconds old
                    oldest_timestamp = self._timestamps[0]
                    wait_time = 60.0 - (current_time - oldest_timestamp)
                else:
                    # We have capacity - grant the token
                    self._timestamps.append(time.time())
                    return

            # Lock is automatically released here when exiting the context manager
            # Wait outside the critical section
            await asyncio.sleep(wait_time)
