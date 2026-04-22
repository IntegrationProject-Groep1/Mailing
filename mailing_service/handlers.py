"""Per-mail-type handlers. Currently only the monitoring alert flow."""

import logging

import sendgrid_client

log = logging.getLogger(__name__)

_DOWN_COLOR = "#d32f2f"
_UP_COLOR = "#2e7d32"


def handle_alert(system: str, status: str, timestamp: str) -> None:
    label = "DOWN" if status == "down" else "UP"
    color = _DOWN_COLOR if status == "down" else _UP_COLOR
    subject = f"[{label}] {system} - {timestamp}"
    html_body = (
        '<!doctype html>'
        '<html><body style="font-family:Arial,sans-serif">'
        f'<h2 style="color:{color};margin:0 0 8px">System {label}: {system}</h2>'
        f'<p><strong>Timestamp (UTC):</strong> {timestamp}</p>'
        '<p>This is an automated alert from the monitoring platform.</p>'
        '</body></html>'
    )

    log.info("Dispatching alert email: system=%s status=%s", system, status)
    sendgrid_client.send_email(subject, html_body)
