"""Handle ``system_alert`` messages from Monitoring (contract §4).

Contract §4 uses a flat ``<alert>`` root (sanctioned exception to Rule 1),
not the standard ``<message>`` envelope. Reads type, system, message, and
timestamp; builds a coloured HTML email and dispatches via the simple
``sendgrid_client.send_email`` path (no template — the alert email is
operator-facing and uses inline HTML).
"""

import logging
import os

import sendgrid_client
from lxml import etree

log = logging.getLogger(__name__)


def handle(raw_body: bytes, schema: etree.XMLSchema) -> None:
    """Validate and send an alert email for one flat ``<alert>`` message.

    Raises whatever ``sendgrid_client`` raises so the caller's nack/ack
    policy stays the single point of truth. Returns normally on validation
    failure (ack-and-discard — a malformed alert must not block the queue).
    """
    try:
        root = etree.fromstring(raw_body)
    except etree.XMLSyntaxError as exc:
        log.warning("system_alert: malformed XML, discarding: %s", exc)
        return

    if not schema.validate(root):
        log.warning("system_alert: schema validation failed, discarding: %s", schema.error_log)
        return

    system = root.findtext("system") or ""
    message = root.findtext("message") or ""
    timestamp = root.findtext("timestamp") or ""

    subject = f"[HEARTBEAT_CRITICAL] {system} — {timestamp}"

    html_body = (
        '<!doctype html>'
        '<html><body style="font-family:Arial,sans-serif">'
        f'<h2 style="color:#d32f2f;margin:0 0 8px">System DOWN: {system}</h2>'
        f'<p><strong>Alert issued (UTC):</strong> {timestamp}</p>'
        f'<p>{message}</p>'
        '<p>This is an automated alert from the monitoring platform.</p>'
        '</body></html>'
    )

    log.info("Dispatching alert email: system=%s", system)
    sendgrid_client.send_email(subject, html_body)
