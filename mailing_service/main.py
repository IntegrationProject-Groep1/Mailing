"""Mailing service consumer.

Consumes alert XML messages from RabbitMQ and dispatches them to the
appropriate handler. The only flow currently wired up is the monitoring
alert flow (queue ``monitoring.alerts``).
"""

import logging
import os
import signal
import sys
import time

import pika
from lxml import etree

import handlers
from sendgrid_client import SendGridError

ALERT_QUEUE = "monitoring.alerts"
CONNECT_RETRIES = 10
CONNECT_DELAY_SECONDS = 2
NACK_BACKOFF_SECONDS = 2

log = logging.getLogger("mailing_service")


def _configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _connection_parameters() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(
        os.environ["RABBITMQ_USER"],
        os.environ["RABBITMQ_PASS"],
    )
    return pika.ConnectionParameters(
        host=os.environ["RABBITMQ_HOST"],
        port=int(os.environ.get("RABBITMQ_PORT", "5672")),
        virtual_host=os.environ.get("RABBITMQ_VHOST", "/"),
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )


def connect() -> pika.BlockingConnection:
    params = _connection_parameters()
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            connection = pika.BlockingConnection(params)
            log.info("Connected to RabbitMQ at %s:%s", params.host, params.port)
            return connection
        except pika.exceptions.AMQPConnectionError:
            log.warning(
                "Connection attempt %d/%d failed, retrying in %ds",
                attempt, CONNECT_RETRIES, CONNECT_DELAY_SECONDS,
            )
            time.sleep(CONNECT_DELAY_SECONDS)
    raise RuntimeError("Could not connect to RabbitMQ after all retries")


def load_schema() -> etree.XMLSchema:
    path = os.getenv("ALERT_XSD_PATH", "/app/alert.xsd")
    log.info("Loading alert XSD from %s", path)
    return etree.XMLSchema(etree.parse(path))


def _build_callback(schema: etree.XMLSchema):
    def on_message(ch, method, _properties, body):
        delivery_tag = method.delivery_tag
        try:
            doc = etree.fromstring(body)
        except etree.XMLSyntaxError as exc:
            log.warning("Malformed XML, discarding: %s", exc)
            ch.basic_ack(delivery_tag)
            return

        if not schema.validate(doc):
            log.warning("XSD validation failed, discarding: %s", schema.error_log)
            ch.basic_ack(delivery_tag)
            return

        system = doc.findtext("system")
        status = doc.findtext("status")
        timestamp = doc.findtext("timestamp")

        try:
            handlers.handle_alert(system, status, timestamp)
            ch.basic_ack(delivery_tag)
        except SendGridError as exc:
            log.error("Transient send failure, requeueing: %s", exc)
            ch.basic_nack(delivery_tag, requeue=True)
            time.sleep(NACK_BACKOFF_SECONDS)
        except RuntimeError as exc:
            log.error("Permanent send failure, discarding: %s", exc)
            ch.basic_ack(delivery_tag)
        except Exception:
            log.exception("Unexpected error handling message; discarding")
            ch.basic_ack(delivery_tag)

    return on_message


def _install_signal_handlers(connection: pika.BlockingConnection, channel) -> None:
    def _graceful(signum, _frame):
        log.info("Received signal %s, stopping consumer", signum)
        try:
            channel.stop_consuming()
        except Exception:
            log.exception("Error stopping channel")
        try:
            connection.close()
        except Exception:
            log.exception("Error closing connection")

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)


def run() -> None:
    schema = load_schema()
    while True:
        connection = connect()
        try:
            channel = connection.channel()
            channel.queue_declare(queue=ALERT_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=ALERT_QUEUE,
                on_message_callback=_build_callback(schema),
                auto_ack=False,
            )
            _install_signal_handlers(connection, channel)
            log.info("Consuming queue %s", ALERT_QUEUE)
            channel.start_consuming()
            log.info("Consumer stopped cleanly, exiting")
            return
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as exc:
            log.error("Broker connection lost: %s. Reconnecting...", exc)
            try:
                connection.close()
            except Exception:
                pass
            time.sleep(CONNECT_DELAY_SECONDS)


def main() -> None:
    _configure_logging()
    try:
        run()
    except Exception:
        log.exception("Fatal error in mailing service")
        sys.exit(1)


if __name__ == "__main__":
    main()
