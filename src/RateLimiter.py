import asyncio
import time


class RateLimiter:
    """
    Rate limiter using Sliding Window Log algorithm.
    Enforces configurable request limits per time window for Steam Web API safety.
    """

    def __init__(self, max_requests: int = 15, window_seconds: float = 60.0):
        """
        Initialize the rate limiter.

        Args:
            max_requests: Maximum requests allowed per window
            window_seconds: Time window in seconds
        """
        self._lock = asyncio.Lock()
        self._timestamps: list[float] = []
        self._max_requests = max_requests
        self._window_seconds = window_seconds

    async def acquire_token(self) -> None:
        """
        Acquire a token to make a request, waiting if necessary to respect rate limits.

        This method ensures that no more than max_requests occur within any window.
        If the limit is reached, it waits until a slot becomes available.
        """
        while True:
            async with self._lock:
                current_time = time.time()
                cutoff_time = current_time - self._window_seconds
                self._timestamps = [ts for ts in self._timestamps if ts > cutoff_time]

                # Check if we've hit the rate limit
                if len(self._timestamps) >= self._max_requests:
                    # Calculate exact wait time until oldest timestamp exits the window
                    oldest_timestamp = self._timestamps[0]
                    wait_time = self._window_seconds - (current_time - oldest_timestamp)
                else:
                    # We have capacity - grant the token
                    self._timestamps.append(time.time())
                    return

            # Lock is automatically released here when exiting the context manager
            # Wait outside the critical section
            await asyncio.sleep(wait_time)
