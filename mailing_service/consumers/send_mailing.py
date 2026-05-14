"""Handle ``send_mailing`` messages from CRM and Facturatie (contract §12.1, §13.1).

Single handler used by both source queues; the only on-the-wire difference
is the ``source`` field which the XSD already enforces.

Error policy (matches the contract's at-least-once semantics):

* ``SendGridError`` (5xx / network) is logged to Monitoring and converted to
  a failed ``mailing_status``; the original message is acked.
* ``UnknownMailTypeError`` / ``MissingTemplateError`` / oversized
  attachment → ack-and-discard, plus a ``mailing_status`` with
  ``status=failed`` and a contract ``log`` so Monitoring sees it.
* SendGrid 4xx (per-batch reject) → ack, publish a ``mailing_status``
  with ``status=failed`` and the rejected addresses; no separate error log
  (the message was well-formed, the receiver just refused it).
"""

import json
import logging
import os

from cachetools import TTLCache

import sendgrid_client
import sendgrid_failures
import templates
from envelope import Envelope
from publishers import logs, mailing_status

log = logging.getLogger(__name__)

# 25 MB, leaving 5 MB headroom under SendGrid's 30 MB total-message cap.
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024

# Track processed and pending messages with a 24-hour TTL to prevent memory leaks.
COMPLETED_MESSAGE_IDS: TTLCache[str, bool] = TTLCache(maxsize=100000, ttl=86400)
_PENDING_STATUSES: TTLCache[str, dict] = TTLCache(maxsize=100000, ttl=86400)


class OversizedAttachmentError(Exception):
    pass


class RetryableStatusPublishError(Exception):
    """Raised when status publication failed after irreversible processing."""


def reset_idempotency_state() -> None:
    """Clear in-memory idempotency state. Intended for tests."""
    COMPLETED_MESSAGE_IDS.clear()
    _PENDING_STATUSES.clear()


def parse_recipients(body) -> list[sendgrid_client.Recipient]:
    recipients_el = body.find("recipients")
    out: list[sendgrid_client.Recipient] = []
    if recipients_el is None:
        return out
    for r in recipients_el.findall("recipient"):
        contact = r.find("contact")
        out.append(
            sendgrid_client.Recipient(
                email=r.findtext("email") or "",
                user_id=r.findtext("identity_uuid") or r.findtext("user_id") or "",
                first_name=(contact.findtext("first_name") if contact is not None else "") or "",
                last_name=(contact.findtext("last_name") if contact is not None else "") or "",
            )
        )
    return out


def parse_attachment(body) -> sendgrid_client.Attachment | None:
    """Extract + size-check a single optional ``<attachment>`` element.

    Raises :class:`OversizedAttachmentError` if the decoded size exceeds
    :data:`MAX_ATTACHMENT_BYTES`. We use the base64 string length as an
    upper bound on the decoded size — cheaper than full decoding and good
    enough for a guard.
    """
    el = body.find("attachment")
    if el is None:
        return None

    b64 = el.findtext("base64_data") or ""
    decoded_bytes_upper = (len(b64) * 3) // 4
    if decoded_bytes_upper > MAX_ATTACHMENT_BYTES:
        raise OversizedAttachmentError(
            f"attachment ~{decoded_bytes_upper} bytes exceeds cap of {MAX_ATTACHMENT_BYTES}"
        )

    return sendgrid_client.Attachment(
        filename=el.findtext("filename") or "",
        content_type=el.findtext("content_type") or "",
        base64_data=b64,
    )


def _publish_failure(
    channel,
    *,
    env: Envelope,
    campaign_id: str,
    subject: str,
    sent: int,
    bounced_emails: list[str] | None = None,
) -> None:
    _publish_final_status(
        channel,
        env=env,
        correlation_id=env.correlation_id,
        campaign_id=campaign_id,
        subject=subject,
        sent=sent,
        delivered=0,
        bounced=sent,
        opened=0,
        bounced_emails=bounced_emails,
        status="failed",
    )


def _publish_final_status(channel, *, env: Envelope, **status_payload) -> None:
    """Publish final status and remember enough state to avoid duplicate sends."""
    if env.message_id:
        _PENDING_STATUSES[env.message_id] = dict(status_payload)
    try:
        mailing_status.publish(channel, **status_payload)
    except Exception as exc:
        raise RetryableStatusPublishError(
            f"failed to publish mailing_status for message_id={env.message_id}"
        ) from exc

    if env.message_id:
        _PENDING_STATUSES.pop(env.message_id, None)
        COMPLETED_MESSAGE_IDS[env.message_id] = True


def _publish_pending_status(channel, env: Envelope) -> bool:
    payload = _PENDING_STATUSES.get(env.message_id)
    if payload is None:
        return False

    log.info("Publishing pending mailing_status without resending email: message_id=%s", env.message_id)
    _publish_final_status(channel, env=env, **payload)
    return True


def handle(env: Envelope, channel) -> None:
    """Process one ``send_mailing`` envelope.

    Returns normally on permanent success or permanent failure (caller
    acks). SendGrid failures are converted to failed statuses and log
    messages so provider outages do not create an infinite requeue loop.
    """
    if env.message_id in COMPLETED_MESSAGE_IDS:
        log.info("Skipping duplicate completed send_mailing message_id=%s", env.message_id)
        return
    if _publish_pending_status(channel, env):
        return

    body = env.body
    campaign_id = body.findtext("campaign_id") or ""
    subject = body.findtext("subject") or ""
    mail_type = body.findtext("mail_type") or ""
    template_data_str = body.findtext("template_data")
    body_html = body.findtext("body_html")
    recipients = parse_recipients(body)
    sent = len(recipients)

    # Parse attachment first so an oversized one is rejected before we
    # even resolve the template — cheap fail-fast.
    try:
        attachment = parse_attachment(body)
    except OversizedAttachmentError as exc:
        log.warning("send_mailing rejected: %s (campaign=%s)", exc, campaign_id)
        logs.publish_system_error(
            channel,
            error_code=logs.INVALID_XML_FORMAT,
            error_description=str(exc),
            related_message_id=env.message_id,
            action="xml_validation",
        )
        _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
        return
    attachments = [attachment] if attachment else None

    # Resolve the SendGrid template; an unknown or unconfigured mail_type
    # is permanent — no point requeuing.
    try:
        template_id = templates.resolve_template_id(mail_type)
    except templates.UnknownMailTypeError as exc:
        log.warning("Unknown mail_type %r (campaign=%s)", mail_type, campaign_id)
        logs.publish_system_error(
            channel,
            error_code=logs.UNKNOWN_MESSAGE_TYPE,
            error_description=f"unknown mail_type: {exc}",
            related_message_id=env.message_id,
        )
        _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
        return
    except templates.MissingTemplateError as exc:
        # Configuration error (env var unset). Same outcome as unknown
        # — don't requeue, surface to Monitoring, return failed status.
        log.error("Missing SendGrid template id: %s", exc)
        logs.publish_system_error(
            channel,
            error_code=logs.UNKNOWN_MESSAGE_TYPE,
            error_description=str(exc),
            related_message_id=env.message_id,
        )
        _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
        return

    template_data: dict | None = None
    if template_data_str:
        try:
            template_data = json.loads(template_data_str)
        except json.JSONDecodeError as exc:
            log.warning("Invalid template_data JSON (campaign=%s): %s", campaign_id, exc)
            logs.publish_system_error(
                channel,
                error_code=logs.INVALID_XML_FORMAT,
                error_description=f"template_data is not valid JSON: {exc}",
                related_message_id=env.message_id,
                action="xml_validation",
            )
            _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
            return

    log.info(
        "Dispatching send_mailing campaign=%s mail_type=%s recipients=%d source=%s",
        campaign_id, mail_type, sent, env.source,
    )

    try:
        result = sendgrid_client.send_template_email(
            template_id=template_id,
            from_email=os.environ["FROM_EMAIL"],
            recipients=recipients,
            template_data=template_data,
            body_html=body_html,
            attachments=attachments,
        )
    except sendgrid_client.SendGridError as exc:
        log.error("SendGrid failed for campaign=%s; marking failed: %s", campaign_id, exc)
        sendgrid_failures.publish_failure_log(
            channel,
            error_description=f"SendGrid send failed for campaign={campaign_id}: {exc}",
            related_message_id=env.message_id,
        )
        _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
        return

    if result.rejected:
        log.warning("SendGrid rejected batch for campaign=%s", campaign_id)
        _publish_final_status(
            channel,
            env=env,
            correlation_id=env.correlation_id,
            campaign_id=campaign_id,
            subject=subject,
            sent=sent,
            delivered=0,
            bounced=sent,
            opened=0,
            bounced_emails=result.rejected,
            status="failed",
        )
        return

    # Initial-cut accounting (see plan §1.7): no per-recipient feedback
    # without the SendGrid Event Webhook, so delivered=sent, bounced=0.
    # TODO(phase4): switch to webhook-driven accurate counts.
    _publish_final_status(
        channel,
        env=env,
        correlation_id=env.correlation_id,
        campaign_id=campaign_id,
        subject=subject,
        sent=sent,
        delivered=sent,
        bounced=0,
        opened=0,
        status="completed",
    )
    logs.publish(
        channel,
        level="info",
        action="email",
        message=f"Successfully sent email campaign={campaign_id} to {sent} recipients",
    )
