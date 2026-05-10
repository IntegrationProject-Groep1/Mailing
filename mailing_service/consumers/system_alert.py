"""Handle ``system_alert`` messages from Monitoring (contract §4).

Contract §4 uses a flat ``<alert>`` root (sanctioned exception to Rule 1),
not the standard ``<message>`` envelope. Reads type, system, message, and
timestamp; builds a coloured HTML email and dispatches via the simple
``sendgrid_client.send_email`` path (no template — the alert email is
operator-facing and uses inline HTML).
"""

import logging
from html import escape

import sendgrid_client
from lxml import etree

log = logging.getLogger(__name__)


class AlertValidationError(Exception):
    """Raised when a flat Monitoring alert is malformed or schema-invalid."""


def parse_alert(raw_body: bytes, schema: etree.XMLSchema) -> tuple[str, str, str]:
    """Validate a flat ``<alert>`` message and return system/message/timestamp."""
    try:
        root = etree.fromstring(raw_body)
    except etree.XMLSyntaxError as exc:
        raise AlertValidationError(f"malformed XML: {exc}") from exc

    if not schema.validate(root):
        raise AlertValidationError(f"schema validation failed: {schema.error_log}")

    return (
        root.findtext("system") or "",
        root.findtext("message") or "",
        root.findtext("timestamp") or "",
    )


def handle(raw_body: bytes, schema: etree.XMLSchema) -> None:
    """Validate and send an alert email for one flat ``<alert>`` message.

    Raises whatever ``sendgrid_client`` raises so the caller's nack/ack
    policy stays the single point of truth. Raises
    :class:`AlertValidationError` on malformed/schema-invalid alerts.
    """
    system, message, timestamp = parse_alert(raw_body, schema)
    safe_system = escape(system)
    safe_message = escape(message)
    safe_timestamp = escape(timestamp)

    subject = f"[HEARTBEAT_CRITICAL] {system} — {timestamp}"

    html_body = (
        '<!doctype html>'
        '<html><body style="font-family:Arial,sans-serif">'
        f'<h2 style="color:#d32f2f;margin:0 0 8px">System DOWN: {safe_system}</h2>'
        f'<p><strong>Alert issued (UTC):</strong> {safe_timestamp}</p>'
        f'<p>{safe_message}</p>'
        '<p>This is an automated alert from the monitoring platform.</p>'
        '</body></html>'
    )

    log.info("Dispatching alert email: system=%s", system)
    sendgrid_client.send_email(subject, html_body)
