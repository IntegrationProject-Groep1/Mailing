"""Publish fake alert XMLs to the mailing service queue.

Runs on the host against the local RabbitMQ broker started by
test/docker-compose.yml. Reads broker coordinates from the environment
(defaults: localhost:5672, user mailing / pass mailing).

Examples:
    python test_alerts.py down
    python test_alerts.py up
    python test_alerts.py malformed
    python test_alerts.py all --count 3
"""

import argparse
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pika

QUEUE = "monitoring.alerts"

log = logging.getLogger("test_alerts")


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_alert(system: str, status: str, timestamp: str | None = None) -> bytes:
    root = ET.Element("alert")
    ET.SubElement(root, "system").text = system
    ET.SubElement(root, "status").text = status
    ET.SubElement(root, "timestamp").text = timestamp or _now_utc()
    return ET.tostring(root, encoding="utf-8")


def _connect(retries: int = 10, delay: int = 2) -> tuple:
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
            channel.queue_declare(queue=QUEUE, durable=True)
            log.info("Connected to RabbitMQ at %s:%s", host, port)
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            log.warning("Connection attempt %d/%d failed, retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    log.error("Could not connect to RabbitMQ after all retries")
    sys.exit(1)


def _publish(channel, body: bytes, label: str) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.info("Sent %s (%d bytes)", label, len(body))


def _publish_down(channel) -> None:
    _publish(channel, _build_alert("facturatie", "down"), "down alert")


def _publish_up(channel) -> None:
    _publish(channel, _build_alert("facturatie", "up"), "up alert")


def _publish_malformed(channel) -> None:
    _publish(channel, b"<alert><system>broken</system>", "truncated XML")
    _publish(channel, _build_alert("facturatie", "broken"), "XSD-invalid status")


SCENARIOS = {
    "down": _publish_down,
    "up": _publish_up,
    "malformed": _publish_malformed,
}


def main() -> None:
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=[*SCENARIOS, "all"])
    parser.add_argument("--count", type=int, default=1, help="Repeat the scenario N times")
    args = parser.parse_args()

    connection, channel = _connect()
    try:
        scenarios = SCENARIOS.values() if args.scenario == "all" else [SCENARIOS[args.scenario]]
        for _ in range(args.count):
            for fn in scenarios:
                fn(channel)
    finally:
        connection.close()
    log.info("Done")


if __name__ == "__main__":
    main()
