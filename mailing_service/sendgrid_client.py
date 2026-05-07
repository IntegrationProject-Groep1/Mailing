"""SendGrid wrapper used by the consumer modules.

Two public entry points:

* :func:`send_email` — plain HTML to ADMIN_EMAILS. Used by ``system_alert``
  (operator-facing fire-and-forget). Raises ``SendGridError`` on transient
  failures and ``RuntimeError`` on permanent rejections so the consumer can
  decide nack-vs-ack.
* :func:`send_template_email` — SendGrid Dynamic Template multi-recipient
  send used by ``send_mailing``. Returns a :class:`SendResult` instead of
  raising on 4xx, because the caller has to publish a ``mailing_status``
  back to CRM either way and needs structured info to fill it in.
"""

import logging
import os
from dataclasses import dataclass, field

from python_http_client.exceptions import HTTPError
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Attachment as SGAttachment,
    Disposition,
    Email,
    FileContent,
    FileName,
    FileType,
    Mail,
    Personalization,
    To,
)

log = logging.getLogger(__name__)


class SendGridError(Exception):
    """Transient / retryable SendGrid failure (5xx, network, timeout)."""


@dataclass(frozen=True)
class Recipient:
    """One recipient extracted from a ``send_mailing`` body."""

    email: str
    user_id: str
    first_name: str
    last_name: str

    @property
    def display_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


@dataclass(frozen=True)
class Attachment:
    """File attachment as it appears in the inbound XML — base64 already."""

    filename: str
    content_type: str
    base64_data: str


@dataclass
class SendResult:
    """Outcome of one ``send_template_email`` call.

    The SendGrid sync API returns a single status code for the whole batch,
    so per-recipient ``accepted`` vs ``rejected`` is currently all-or-nothing
    until we wire the SendGrid Event Webhook (Phase 4).
    """

    accepted: list[str] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)


def _recipients() -> list[str]:
    raw = os.environ["ADMIN_EMAILS"]
    return [e.strip() for e in raw.split(",") if e.strip()]


def send_email(subject: str, html_body: str) -> None:
    """Send a plain HTML email to ADMIN_EMAILS. Used by system_alert."""
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


def _build_attachment(att: Attachment) -> SGAttachment:
    a = SGAttachment()
    a.file_content = FileContent(att.base64_data)
    a.file_name = FileName(att.filename)
    a.file_type = FileType(att.content_type)
    a.disposition = Disposition("attachment")
    return a


def send_template_email(
    *,
    template_id: str,
    from_email: str,
    recipients: list[Recipient],
    template_data: dict | None = None,
    body_html: str | None = None,
    attachments: list[Attachment] | None = None,
    subject: str | None = None,
) -> SendResult:
    """Send a SendGrid Dynamic Template to one or more recipients.

    One ``Mail`` with one ``Personalization`` per recipient → one HTTP call,
    not N. Per contract sections 12.1/13.1.

    Returns :class:`SendResult` describing accepted/rejected addresses.
    Raises :class:`SendGridError` on 5xx/network failures so the consumer
    can nack-requeue the inbound message.
    """
    if not recipients:
        raise RuntimeError("send_template_email called with no recipients")

    api_key = os.environ["SENDGRID_API_KEY"]

    message = Mail(from_email=Email(from_email))
    message.template_id = template_id
    if subject:
        message.subject = subject
    if body_html:
        # Fallback if the template ever fails to resolve; harmless when the
        # template is present (template wins).
        message.add_content("text/html", body_html)

    for r in recipients:
        p = Personalization()
        p.add_to(To(r.email, name=r.display_name or None))
        if template_data:
            p.dynamic_template_data = template_data
        message.add_personalization(p)

    for att in attachments or []:
        message.add_attachment(_build_attachment(att))

    client = SendGridAPIClient(api_key)
    try:
        response = client.send(message)
    except HTTPError as exc:
        status = getattr(exc, "status_code", None)
        if status is not None and 400 <= status < 500:
            log.warning("SendGrid 4xx rejected batch (%s): %r", status, exc.body)
            return SendResult(
                accepted=[],
                rejected=[r.email for r in recipients],
            )
        raise SendGridError(f"SendGrid HTTP error: {exc}") from exc
    except (TimeoutError, OSError) as exc:
        raise SendGridError(f"SendGrid network error: {exc}") from exc

    status = response.status_code
    if 500 <= status < 600:
        raise SendGridError(f"SendGrid server error: {status}")
    if not (200 <= status < 300):
        log.warning("SendGrid non-2xx (%s): %r", status, response.body)
        return SendResult(accepted=[], rejected=[r.email for r in recipients])

    log.info(
        "SendGrid accepted template=%s status=%s recipients=%d",
        template_id, status, len(recipients),
    )
    return SendResult(accepted=[r.email for r in recipients], rejected=[])
