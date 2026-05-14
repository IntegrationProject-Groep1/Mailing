"""Handle ``send_mailing`` messages from Monitoring (queue ``monitoring.reports``).

Monitoring publishes a daily platform report XML once per day; mailing
dispatches it to the recipients listed in the envelope via a SendGrid
Dynamic Template and reports the outcome to the shared ``logs`` queue.

Differences from the CRM/Facturatie ``send_mailing`` consumer:

* The wire schema is ``monitoring_report.xsd`` (recipient ``user_id`` is a
  free-form string, ``mail_type`` is locked to ``daily_report``, the inline
  ``template_id`` element is accepted but ignored).
* No ``mailing_status`` reply is published — monitoring does not consume
  one. Success/failure surfaces on the ``logs`` queue only, mirroring the
  ``system_alert`` consumer's logging-only pattern.
* The recipient parsing, attachment size cap, and 24-hour idempotency
  cache are reused from :mod:`consumers.send_mailing` to keep one source
  of truth.

Error policy:

* ``SendGridError`` (5xx / network) propagates so the caller's standard
  handler in :func:`main._build_callback` logs and acks the message.
* Oversized attachment / unknown / unconfigured mail_type / invalid
  ``template_data`` JSON → log a contract ``system_error`` and return
  (ack, no requeue).
"""

import json
import logging
import os

import sendgrid_client
import templates
from consumers.send_mailing import (
    COMPLETED_MESSAGE_IDS,
    OversizedAttachmentError,
    parse_attachment,
    parse_recipients,
)
from envelope import Envelope
from publishers import logs

log = logging.getLogger(__name__)


def handle(env: Envelope, channel) -> None:
    """Process one monitoring daily-report envelope.

    Returns normally on permanent success or permanent failure (caller
    acks). SendGrid 5xx/network failures propagate so the shared callback
    can convert them into a contract failure log.
    """
    if env.message_id in COMPLETED_MESSAGE_IDS:
        log.info("Skipping duplicate completed monitoring_report message_id=%s", env.message_id)
        return

    body = env.body
    campaign_id = body.findtext("campaign_id") or ""
    subject = body.findtext("subject") or ""
    mail_type = body.findtext("mail_type") or ""
    template_data_str = body.findtext("template_data")
    recipients = parse_recipients(body)
    sent = len(recipients)

    # Reject oversized attachments before we touch SendGrid.
    try:
        attachment = parse_attachment(body)
    except OversizedAttachmentError as exc:
        log.warning("monitoring_report rejected: %s (campaign=%s)", exc, campaign_id)
        logs.publish_system_error(
            channel,
            error_code=logs.INVALID_XML_FORMAT,
            error_description=str(exc),
            related_message_id=env.message_id,
            action="xml_validation",
        )
        return
    attachments = [attachment] if attachment else None

    # Resolve the SendGrid template from env (the inline <template_id>
    # element is intentionally ignored — real template ids stay in
    # mailing's deployment config, not in cross-service payloads).
    try:
        template_id = templates.resolve_template_id(mail_type)
    except templates.UnknownMailTypeError as exc:
        # The XSD should already have rejected this; defense in depth.
        log.warning("Unknown mail_type %r (campaign=%s)", mail_type, campaign_id)
        logs.publish_system_error(
            channel,
            error_code=logs.UNKNOWN_MESSAGE_TYPE,
            error_description=f"unknown mail_type: {exc}",
            related_message_id=env.message_id,
        )
        return
    except templates.MissingTemplateError as exc:
        log.error("Missing SendGrid template id: %s", exc)
        logs.publish_system_error(
            channel,
            error_code=logs.UNKNOWN_MESSAGE_TYPE,
            error_description=str(exc),
            related_message_id=env.message_id,
        )
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
            return

    log.info(
        "Dispatching monitoring_report campaign=%s recipients=%d attachment=%s",
        campaign_id, sent, "yes" if attachment else "no",
    )

    # SendGridError propagates — main._build_callback owns the failure-log
    # + ack policy for transient SendGrid issues. A 4xx batch rejection
    # comes back via result.rejected and is treated as a permanent failure
    # (logged, acked, no requeue).
    result = sendgrid_client.send_template_email(
        template_id=template_id,
        from_email=os.environ["FROM_EMAIL"],
        recipients=recipients,
        template_data=template_data,
        attachments=attachments,
        subject=subject,
    )

    if result.rejected:
        log.warning(
            "SendGrid rejected monitoring_report batch campaign=%s rejected=%s",
            campaign_id, result.rejected,
        )
        COMPLETED_MESSAGE_IDS[env.message_id] = True
        logs.publish(
            channel,
            level="warning",
            action="email",
            message=(
                f"SendGrid rejected all recipients for campaign={campaign_id} "
                f"(permanent delivery failure): {', '.join(result.rejected)}"
            ),
        )
        return

    COMPLETED_MESSAGE_IDS[env.message_id] = True
    logs.publish(
        channel,
        level="info",
        action="email",
        message=(
            f"Successfully sent daily report campaign={campaign_id} "
            f"to {sent} recipients"
        ),
    )
