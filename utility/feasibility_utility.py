"""Single source of feasibility truth.

Shared by every actor that gates the tracked set against the rate budget:
  * cerebro boot validation (hard-exits on an infeasible config)
  * the scheduler's NOTIFY listener (the final runtime gate)
  * the write endpoints' pre-check (synchronous answer to the user)

Keep it keyed on plain poll-interval integers, not on any dict shape, so the
ingestion side (items carrying 'polling-interval-in-seconds') and the API side
(rows carrying poll_interval_sec) both feed it without coupling — one rule, no
drift between two implementations.
"""


def compute_feasibility(rate_limit: int, window_seconds: int, poll_intervals: list[int]) -> tuple[bool, int, float]:
    """Worst-case sustained-demand check.

    Sums window // interval per item (max requests each item issues in one
    window) and compares to the budget. Returns
    (feasible, total_reqs_per_window, utilization_pct). No printing, no exit —
    callers decide what to do, so the same rule that hard-exits at boot can
    merely reject a live change or a request.
    """
    total_reqs = 0
    for interval in poll_intervals:
        total_reqs += window_seconds // interval
    utilization = (total_reqs / rate_limit) * 100 if rate_limit else float('inf')
    return total_reqs <= rate_limit, total_reqs, utilization
