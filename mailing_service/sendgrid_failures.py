"""Shared SendGrid failure logging and outage escalation."""

from failure_tracker import SlidingWindowFailureTracker
from publishers import logs

_SENDGRID_FAILURES = SlidingWindowFailureTracker(
    window_seconds=60.0, threshold=3, cooldown_seconds=300.0,
)


def publish_failure_log(
    channel,
    *,
    error_description: str,
    related_message_id: str | None = None,
) -> None:
    """Log one SendGrid failure and a rate-limited outage escalation."""
    logs.publish_system_error(
        channel,
        error_code=logs.SENDGRID_UNAVAILABLE,
        error_description=error_description,
        related_message_id=related_message_id,
        action="email",
    )
    if _SENDGRID_FAILURES.record_failure():
        logs.publish_system_error(
            channel,
            error_code=logs.SENDGRID_UNAVAILABLE,
            error_description=(
                "SendGrid 5xx/network failures crossed 3-in-60s threshold "
                f"(latest: {error_description})"
            ),
            related_message_id=related_message_id,
        )


def reset_failure_tracker() -> None:
    """Reset SendGrid failure escalation state. Intended for tests."""
    _SENDGRID_FAILURES.reset()
