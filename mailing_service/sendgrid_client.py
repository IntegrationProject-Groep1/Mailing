"""SendGrid wrapper used by the mailing service handlers."""

import logging
import os

from python_http_client.exceptions import HTTPError
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

log = logging.getLogger(__name__)


class SendGridError(Exception):
    """Transient / retryable SendGrid failure (5xx, network, timeout)."""


def _recipients() -> list[str]:
    raw = os.environ["ADMIN_EMAILS"]
    return [e.strip() for e in raw.split(",") if e.strip()]


def send_email(subject: str, html_body: str) -> None:
    api_key = os.environ["SENDGRID_API_KEY"]
    from_email = os.environ["FROM_EMAIL"]
    to_emails = _recipients()
    if not to_emails:
        raise RuntimeError("ADMIN_EMAILS is empty; nothing to send to")

    message = Mail(
        from_email=from_email,
        to_emails=to_emails,
        subject=subject,
        html_content=html_body,
    )

    client = SendGridAPIClient(api_key)
    try:
        response = client.send(message)
    except HTTPError as exc:
        status = getattr(exc, "status_code", None)
        if status is not None and 400 <= status < 500:
            raise RuntimeError(f"SendGrid rejected message ({status}): {exc.body!r}") from exc
        raise SendGridError(f"SendGrid HTTP error: {exc}") from exc
    except (TimeoutError, OSError) as exc:
        raise SendGridError(f"SendGrid network error: {exc}") from exc

    status = response.status_code
    if 500 <= status < 600:
        raise SendGridError(f"SendGrid server error: {status}")
    if not (200 <= status < 300):
        raise RuntimeError(f"SendGrid rejected message ({status}): {response.body!r}")

    log.info("SendGrid accepted message (status=%s) for %d recipient(s)", status, len(to_emails))
