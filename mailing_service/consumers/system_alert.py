"""Handle ``system_alert`` messages from Monitoring (contract §4).

Replaces the old ``handlers.handle_alert`` flow. Reads the v2.0 envelope
fields, builds a coloured HTML email, and dispatches via the simple
``sendgrid_client.send_email`` path (no template — the alert email is
operator-facing and uses inline HTML).
"""

import logging

import sendgrid_client
from envelope import Envelope

log = logging.getLogger(__name__)

# Heading colours: red for offline, green for online.
_COLORS = {
    "online": "#2e7d32",
    "offline": "#d32f2f",
}


def handle(env: Envelope) -> None:
    """Send an alert email for one ``system_alert`` envelope.

    Raises whatever ``sendgrid_client`` raises so the consumer's
    nack/ack policy in ``main.py`` stays the single point of truth.
    """
    system = env.body.findtext("system") or ""
    status = env.body.findtext("status") or ""
    last_seen = env.body.findtext("last_seen")
    label = status.upper()
    color = _COLORS.get(status, "#555555")

    subject = f"[{label}] {system} - {env.timestamp}"

    last_seen_html = (
        f'<p><strong>Last seen (UTC):</strong> {last_seen}</p>' if last_seen else ""
    )

    html_body = (
        '<!doctype html>'
        '<html><body style="font-family:Arial,sans-serif">'
        f'<h2 style="color:{color};margin:0 0 8px">System {label}: {system}</h2>'
        f'<p><strong>Alert issued (UTC):</strong> {env.timestamp}</p>'
        f'{last_seen_html}'
        '<p>This is an automated alert from the monitoring platform.</p>'
        '</body></html>'
    )

    log.info("Dispatching alert email: system=%s status=%s", system, status)
    sendgrid_client.send_email(subject, html_body)
