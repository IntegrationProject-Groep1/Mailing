"""Handle ``send_mailing`` messages from CRM and Facturatie (contract §12.1, §13.1).

Single handler used by both source queues; the only on-the-wire difference
is the ``source`` field which the XSD already enforces.

Error policy (matches the contract's at-least-once semantics):

* ``SendGridError`` (5xx / network) propagates up so ``main.py`` can
  ``nack`` + requeue; the redelivery retries the whole flow.
* ``UnknownMailTypeError`` / ``MissingTemplateError`` / oversized
  attachment → ack-and-discard, plus a ``mailing_status`` with
  ``status=failed`` and a ``system_error`` so Monitoring sees it.
* SendGrid 4xx (per-batch reject) → ack, publish a ``mailing_status``
  with ``status=failed`` and the rejected addresses; no ``system_error``
  (the message was well-formed, the receiver just refused it).
"""

import json
import logging
import os

import sendgrid_client
import templates
from envelope import Envelope
from publishers import mailing_status, system_error

log = logging.getLogger(__name__)

# 25 MB, leaving 5 MB headroom under SendGrid's 30 MB total-message cap.
MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024


class _OversizedAttachmentError(Exception):
    pass


def _parse_recipients(body) -> list[sendgrid_client.Recipient]:
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


def _parse_attachment(body) -> sendgrid_client.Attachment | None:
    """Extract + size-check a single optional ``<attachment>`` element.

    Raises :class:`_OversizedAttachmentError` if the decoded size exceeds
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
        raise _OversizedAttachmentError(
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
    mailing_status.publish(
        channel,
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


def handle(env: Envelope, channel) -> None:
    """Process one ``send_mailing`` envelope.

    Returns normally on permanent success or permanent failure (caller
    acks). Raises :class:`sendgrid_client.SendGridError` on transient
    SendGrid failures so caller can nack-requeue.
    """
    body = env.body
    campaign_id = body.findtext("campaign_id") or ""
    subject = body.findtext("subject") or ""
    mail_type = body.findtext("mail_type") or ""
    template_data_str = body.findtext("template_data")
    body_html = body.findtext("body_html")
    recipients = _parse_recipients(body)
    sent = len(recipients)

    # Parse attachment first so an oversized one is rejected before we
    # even resolve the template — cheap fail-fast.
    try:
        attachment = _parse_attachment(body)
    except _OversizedAttachmentError as exc:
        log.warning("send_mailing rejected: %s (campaign=%s)", exc, campaign_id)
        system_error.publish(
            channel,
            error_code=system_error.INVALID_XML_FORMAT,
            error_description=str(exc),
            related_message_id=env.message_id,
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
        system_error.publish(
            channel,
            error_code=system_error.UNKNOWN_MESSAGE_TYPE,
            error_description=f"unknown mail_type: {exc}",
            related_message_id=env.message_id,
        )
        _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
        return
    except templates.MissingTemplateError as exc:
        # Configuration error (env var unset). Same outcome as unknown
        # — don't requeue, surface to Monitoring, return failed status.
        log.error("Missing SendGrid template id: %s", exc)
        system_error.publish(
            channel,
            error_code=system_error.UNKNOWN_MESSAGE_TYPE,
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
            system_error.publish(
                channel,
                error_code=system_error.INVALID_XML_FORMAT,
                error_description=f"template_data is not valid JSON: {exc}",
                related_message_id=env.message_id,
            )
            _publish_failure(channel, env=env, campaign_id=campaign_id, subject=subject, sent=sent)
            return

    log.info(
        "Dispatching send_mailing campaign=%s mail_type=%s recipients=%d source=%s",
        campaign_id, mail_type, sent, env.source,
    )

    # SendGridError (5xx/network) escapes — main.py nack-requeues.
    result = sendgrid_client.send_template_email(
        template_id=template_id,
        from_email=os.environ["FROM_EMAIL"],
        recipients=recipients,
        template_data=template_data,
        body_html=body_html,
        attachments=attachments,
    )

    if result.rejected:
        log.warning("SendGrid rejected batch for campaign=%s", campaign_id)
        mailing_status.publish(
            channel,
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
    mailing_status.publish(
        channel,
        correlation_id=env.correlation_id,
        campaign_id=campaign_id,
        subject=subject,
        sent=sent,
        delivered=sent,
        bounced=0,
        opened=0,
        status="completed",
    )
