import logging
import os
import signal
import sys
import time

import pika
from lxml import etree

import envelope
from consumers import send_mailing as send_mailing_consumer
from consumers import system_alert as system_alert_consumer
from publishers import system_error
from sendgrid_client import SendGridError

ALERT_QUEUE = "monitoring.alerts"
CRM_SEND_MAILING_QUEUE = "crm.to.mailing"
FACTURATIE_SEND_MAILING_QUEUE = "facturatie.to.mailing"
MAILING_ERROR_QUEUE = system_error.ERROR_QUEUE   # "mailing.errors"

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


def _load_schema(name: str) -> etree.XMLSchema:
    schemas_dir = os.getenv("SCHEMAS_DIR", "/app/schemas")
    path = f"{schemas_dir}/{name}.xsd"
    log.info("Loading XSD: %s", path)
    return etree.XMLSchema(etree.parse(path))


def _build_callback(schema: etree.XMLSchema, handler, *, pass_channel: bool):
    """Build a per-queue callback that validates + dispatches.

    ``handler`` takes either ``(env)`` (system_alert flow) or
    ``(env, channel)`` (send_mailing flow) — controlled by ``pass_channel``.
    On envelope-format failures we publish a ``system_error`` and ack;
    on transient SendGrid failures we nack-requeue; on permanent failures
    we ack (the handler already published the appropriate response).
    """
    def on_message(ch, method, _properties, body):
        delivery_tag = method.delivery_tag
        try:
            env = envelope.parse_and_validate(body, schema)
        except envelope.MalformedXMLError as exc:
            log.warning("Malformed XML on %s, flagging: %s", method.routing_key, exc)
            system_error.publish(
                ch,
                error_code=system_error.INVALID_XML_FORMAT,
                error_description=f"malformed XML on {method.routing_key}: {exc}",
                related_message_id=None,
            )
            ch.basic_ack(delivery_tag)
            return
        except envelope.SchemaValidationError as exc:
            log.warning("XSD validation failed on %s, flagging: %s", method.routing_key, exc)
            system_error.publish(
                ch,
                error_code=system_error.INVALID_XML_FORMAT,
                error_description=f"schema validation failed on {method.routing_key}: {exc}",
                related_message_id=None,
            )
            ch.basic_ack(delivery_tag)
            return

        try:
            if pass_channel:
                handler(env, ch)
            else:
                handler(env)
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
    # Load all per-flow schemas once at startup. The mailing_status and
    # system_error publishers lazy-load their own outbound schemas.
    system_alert_schema = _load_schema("system_alert")
    send_mailing_schema = _load_schema("send_mailing")

    while True:
        connection = connect()
        try:
            channel = connection.channel()
            # Publisher confirms: required so a failed mailing_status publish
            # raises and the original send_mailing gets nack-requeued.
            channel.confirm_delivery()

            # Queues we consume from.
            channel.queue_declare(queue=ALERT_QUEUE, durable=True)
            channel.queue_declare(queue=CRM_SEND_MAILING_QUEUE, durable=True)
            channel.queue_declare(queue=FACTURATIE_SEND_MAILING_QUEUE, durable=True)
            # Queues we publish to. crm.incoming is owned by CRM but we declare
            # it here to ensure it exists before the first publish.
            channel.queue_declare(queue=MAILING_ERROR_QUEUE, durable=True)
            channel.queue_declare(queue="crm.incoming", durable=True)

            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=ALERT_QUEUE,
                on_message_callback=_build_callback(
                    system_alert_schema, system_alert_consumer.handle, pass_channel=False,
                ),
                auto_ack=False,
            )
            channel.basic_consume(
                queue=CRM_SEND_MAILING_QUEUE,
                on_message_callback=_build_callback(
                    send_mailing_schema, send_mailing_consumer.handle, pass_channel=True,
                ),
                auto_ack=False,
            )
            channel.basic_consume(
                queue=FACTURATIE_SEND_MAILING_QUEUE,
                on_message_callback=_build_callback(
                    send_mailing_schema, send_mailing_consumer.handle, pass_channel=True,
                ),
                auto_ack=False,
            )

            _install_signal_handlers(connection, channel)
            log.info(
                "Consuming queues: %s, %s, %s",
                ALERT_QUEUE, CRM_SEND_MAILING_QUEUE, FACTURATIE_SEND_MAILING_QUEUE,
            )
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
