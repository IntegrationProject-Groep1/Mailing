"""Publish v2.0-shaped fake messages to the mailing service queues.

Runs on the host against the local RabbitMQ broker started by
test/docker-compose.yml. Reads broker coordinates from the environment
(defaults: localhost:5672, user mailing / pass mailing).

Examples:
    python test_messages.py --scenario system_alert_offline
    python test_messages.py --scenario crm_send_mailing
    python test_messages.py --scenario all
    python test_messages.py --scenario all --listen-mailing-status --listen-logs --listen-seconds 8
"""

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika

QUEUE_MONITORING_ALERTS = "monitoring.alerts"
QUEUE_CRM_SEND_MAILING = "crm.to.mailing"
QUEUE_FACTURATIE_SEND_MAILING = "facturatie.to.mailing"
QUEUE_CRM_INCOMING = "crm.incoming"
QUEUE_LOGS = "logs"

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

log = logging.getLogger("test_messages")


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _substitute(raw: str) -> bytes:
    """Replace fixture placeholders with fresh per-publish values."""
    return (
        raw
        .replace("{{MESSAGE_ID}}", str(uuid.uuid4()))
        .replace("{{CORRELATION_ID}}", str(uuid.uuid4()))
        .replace("{{TIMESTAMP}}", _now_utc())
    ).encode("utf-8")


def _load_fixture(name: str) -> bytes:
    path = FIXTURES_DIR / f"{name}.xml"
    return _substitute(path.read_text())


def _build_oversized_send_mailing() -> bytes:
    """Build a send_mailing with a >25 MB base64 attachment.

    Verifies the consumer's MAX_ATTACHMENT_BYTES guard (plan §1.5):
    the consumer should ack, publish an error log, and publish a
    mailing_status with status=failed.
    """
    # 26 MiB of base64 'A' characters → decodes to ~19.5 MiB but the
    # consumer's upper-bound check uses len(b64)*3//4 ≈ 19.5 MB. So we
    # need len(b64) such that len(b64)*3//4 > 25 MB → len(b64) > ~33.3 MB.
    big_b64 = "A" * (34 * 1024 * 1024)
    return _substitute(f"""<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>{{{{MESSAGE_ID}}}}</message_id>
    <timestamp>{{{{TIMESTAMP}}}}</timestamp>
    <source>crm</source>
    <type>send_mailing</type>
    <version>2.0</version>
    <correlation_id>{{{{CORRELATION_ID}}}}</correlation_id>
  </header>
  <body>
    <campaign_id>sg-campaign-oversized</campaign_id>
    <subject>Test oversized attachment</subject>
    <mail_type>general_announcement</mail_type>
    <recipients>
      <recipient>
        <email>jan@example.test</email>
        <identity_uuid>e8b27c1d-4f2a-4b3e-9c5f-123456789abc</identity_uuid>
        <contact><first_name>Jan</first_name><last_name>P</last_name></contact>
      </recipient>
    </recipients>
    <attachment>
      <filename>huge.bin</filename>
      <content_type>application/octet-stream</content_type>
      <base64_data>{big_b64}</base64_data>
    </attachment>
  </body>
</message>""")


def _build_unknown_mail_type() -> bytes:
    """A send_mailing with mail_type not in our enum.

    The XSD will REJECT this before the consumer sees it (the mail_type
    enum is enforced by send_mailing.xsd) → behaviour is a logs message
    with invalid_xml_format on schema validation. We test the same code
    path either way; what matters is that no email is sent and a log fires.
    """
    raw = (FIXTURES_DIR / "crm_send_mailing.xml").read_text()
    raw = raw.replace(
        "<mail_type>registration_confirmation</mail_type>",
        "<mail_type>not_a_real_type</mail_type>",
    )
    return _substitute(raw)


SCENARIOS = {
    "system_alert_offline":               (QUEUE_MONITORING_ALERTS,        lambda: _load_fixture("system_alert_offline")),
    "system_alert_online":                (QUEUE_MONITORING_ALERTS,        lambda: _load_fixture("system_alert_online")),
    "legacy_alert":                       (QUEUE_MONITORING_ALERTS,        lambda: _load_fixture("legacy_alert")),
    "malformed_envelope_missing_header":  (QUEUE_MONITORING_ALERTS,        lambda: _load_fixture("malformed_envelope_missing_header")),
    "crm_send_mailing":                   (QUEUE_CRM_SEND_MAILING,         lambda: _load_fixture("crm_send_mailing")),
    "crm_send_mailing_attachment":        (QUEUE_CRM_SEND_MAILING,         lambda: _load_fixture("crm_send_mailing_attachment")),
    "crm_send_mailing_oversized_attachment": (QUEUE_CRM_SEND_MAILING,      _build_oversized_send_mailing),
    "crm_send_mailing_unknown_type":      (QUEUE_CRM_SEND_MAILING,         _build_unknown_mail_type),
    "facturatie_send_mailing":            (QUEUE_FACTURATIE_SEND_MAILING,  lambda: _load_fixture("facturatie_send_mailing")),
}


def _connect(retries: int = 10, delay: int = 2) -> tuple[pika.BlockingConnection, "pika.adapters.blocking_connection.BlockingChannel"]:
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    port = int(os.environ.get("RABBITMQ_PORT", "5672"))
    user = os.environ.get("RABBITMQ_USER", "mailing")
    password = os.environ.get("RABBITMQ_PASS", "mailing")
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(host=host, port=port, virtual_host=vhost, credentials=credentials)

    for attempt in range(1, retries + 1):
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            log.info("Connected to RabbitMQ at %s:%s", host, port)
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            log.warning("Connection attempt %d/%d failed, retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    log.error("Could not connect to RabbitMQ after all retries")
    sys.exit(1)


def _declare_queues(channel) -> None:
    """Declare every queue we publish to or drain — durable=True everywhere.

    The mailing service also declares its own consumed queues; matching
    parameters here keeps the test broker compatible whether the service
    starts before or after this script.
    """
    for q in (
        QUEUE_MONITORING_ALERTS,
        QUEUE_CRM_SEND_MAILING,
        QUEUE_FACTURATIE_SEND_MAILING,
        QUEUE_CRM_INCOMING,
        QUEUE_LOGS,
    ):
        channel.queue_declare(queue=q, durable=True)


def _publish(channel, queue: str, body: bytes, label: str) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.info("Sent %-40s -> %-30s (%d bytes)", label, queue, len(body))


def _drain(channel, queue: str, label: str, seconds: float) -> int:
    """Pop and print every message on ``queue`` within ``seconds`` seconds."""
    end = time.time() + seconds
    received = 0
    while time.time() < end:
        method, _props, body = channel.basic_get(queue=queue, auto_ack=True)
        if method is None:
            time.sleep(0.25)
            continue
        received += 1
        log.info("[%s #%d] %s", label, received, body.decode("utf-8", errors="replace"))
    log.info("[%s] drained %d messages in %.1fs", label, received, seconds)
    return received


def main() -> None:
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=[*SCENARIOS, "all"], required=True)
    parser.add_argument("--count", type=int, default=1, help="Repeat the chosen scenario(s) N times")
    parser.add_argument("--listen-mailing-status", action="store_true",
                        help="After publishing, drain crm.incoming for --listen-seconds")
    parser.add_argument("--listen-logs", action="store_true",
                        help="After publishing, drain logs for --listen-seconds")
    parser.add_argument("--listen-seconds", type=float, default=5.0,
                        help="How long to drain response queues (default: 5)")
    args = parser.parse_args()

    connection, channel = _connect()
    try:
        _declare_queues(channel)

        names = list(SCENARIOS.keys()) if args.scenario == "all" else [args.scenario]
        for _ in range(args.count):
            for name in names:
                queue, build = SCENARIOS[name]
                _publish(channel, queue, build(), name)

        if args.listen_mailing_status:
            _drain(channel, QUEUE_CRM_INCOMING, "mailing_status", args.listen_seconds)
        if args.listen_logs:
            _drain(channel, QUEUE_LOGS, "logs", args.listen_seconds)
    finally:
        connection.close()
    log.info("Done")


if __name__ == "__main__":
    main()
