"""Sliding-window failure tracker used for outage log escalation.

A single failure is just a normal log line; many failures in a short
window indicate a platform-level problem (e.g. SendGrid API auth gone
bad, broker churning) that another team needs to know about. This
module tracks the rate, fires a callback when a threshold trips, and
holds a cooldown afterwards so we don't spam Monitoring's ``logs`` queue.

Used in two places:

* SendGrid 5xx ramp — 3+ failures in 60 s → ``sendgrid_unavailable`` log
* Broker reconnect after sustained outage — counted via this same
  primitive but with different params (see main.py)

The tracker is deliberately not thread-safe in the strict sense — pika's
BlockingConnection runs all delivery callbacks on a single I/O thread
and that's the only caller. If that ever changes, wrap accesses in a
``threading.Lock``.
"""

import time
from collections import deque


class SlidingWindowFailureTracker:
    """Trip when N failures land within ``window_seconds``.

    After tripping, ``record_failure`` returns ``False`` for at least
    ``cooldown_seconds`` to prevent repeated escalations on the same
    sustained outage.
    """

    def __init__(
        self,
        *,
        window_seconds: float = 60.0,
        threshold: int = 3,
        cooldown_seconds: float = 300.0,
    ) -> None:
        self._window = window_seconds
        self._threshold = threshold
        self._cooldown = cooldown_seconds
        self._times: deque[float] = deque()
        self._cooldown_until: float = 0.0

    def record_failure(self, *, now: float | None = None) -> bool:
        """Record one failure; return True iff the threshold just tripped.

        The caller fires the outage log on True. Returns
        False during the cooldown that follows a previous trip even if
        failures keep arriving.
        """
        t = time.monotonic() if now is None else now
        self._times.append(t)

        # Trim entries outside the window.
        cutoff = t - self._window
        while self._times and self._times[0] < cutoff:
            self._times.popleft()

        if t < self._cooldown_until:
            return False
        if len(self._times) < self._threshold:
            return False

        # Tripped. Start the cooldown and clear the window so the next
        # series of failures has to re-cross the threshold from zero.
        self._cooldown_until = t + self._cooldown
        self._times.clear()
        return True

    def reset(self) -> None:
        """Clear the window and cooldown. Call on a clean recovery."""
        self._times.clear()
        self._cooldown_until = 0.0
