import logging
import os
import signal
import sys
import time

import pika
from lxml import etree

import envelope
import sendgrid_failures
import templates
from consumers import send_mailing as send_mailing_consumer
from consumers import system_alert as system_alert_consumer
from publishers import logs
from sendgrid_client import SendGridError

ALERT_QUEUE = "monitoring.alerts"
CRM_SEND_MAILING_QUEUE = "crm.to.mailing"
FACTURATIE_SEND_MAILING_QUEUE = "facturatie.to.mailing"
LOG_QUEUE = logs.LOG_QUEUE

CONNECT_RETRIES = 10
CONNECT_DELAY_SECONDS = 2

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


def validate_startup_config() -> dict[str, etree.XMLSchema]:
    """Validate required environment and schemas before consuming messages."""
    required = [
        "RABBITMQ_HOST",
        "RABBITMQ_USER",
        "RABBITMQ_PASS",
        "SENDGRID_API_KEY",
        "FROM_EMAIL",
        "ADMIN_EMAILS",
    ]
    required.extend(f"SENDGRID_TEMPLATE_{mail_type.name}" for mail_type in templates.MailType)

    missing = [name for name in required if not os.environ.get(name, "").strip()]
    if missing:
        raise RuntimeError(f"Missing required environment variable(s): {', '.join(missing)}")

    try:
        port = int(os.environ.get("RABBITMQ_PORT", "5672"))
    except ValueError as exc:
        raise RuntimeError("RABBITMQ_PORT must be an integer") from exc
    if port <= 0:
        raise RuntimeError("RABBITMQ_PORT must be positive")

    admin_recipients = [e.strip() for e in os.environ["ADMIN_EMAILS"].split(",") if e.strip()]
    if not admin_recipients:
        raise RuntimeError("ADMIN_EMAILS must contain at least one email address")

    return {
        name: _load_schema(name)
        for name in ("system_alert", "send_mailing", "mailing_status", "log")
    }


def _build_callback(schema: etree.XMLSchema, handler, *, pass_channel: bool):
    """Build a per-queue callback that validates + dispatches standard envelopes.

    ``handler`` takes either ``(env)`` or ``(env, channel)`` — controlled by
    ``pass_channel``. On envelope-format failures we publish a contract log and
    ack. Transient SendGrid failures are logged and acked so outages do not
    create an infinite requeue loop.
    """
    def on_message(ch, method, _properties, body):
        delivery_tag = method.delivery_tag
        try:
            env = envelope.parse_and_validate(body, schema)
        except envelope.MalformedXMLError as exc:
            log.warning("Malformed XML on %s, flagging: %s", method.routing_key, exc)
            logs.publish_system_error(
                ch,
                error_code=logs.INVALID_XML_FORMAT,
                error_description=f"malformed XML on {method.routing_key}: {exc}",
                related_message_id=None,
                action="xml_validation",
            )
            ch.basic_ack(delivery_tag)
            return
        except envelope.SchemaValidationError as exc:
            log.warning("XSD validation failed on %s, flagging: %s", method.routing_key, exc)
            logs.publish_system_error(
                ch,
                error_code=logs.INVALID_XML_FORMAT,
                error_description=f"schema validation failed on {method.routing_key}: {exc}",
                related_message_id=None,
                action="xml_validation",
            )
            ch.basic_ack(delivery_tag)
            return

        try:
            if pass_channel:
                handler(env, ch)
            else:
                handler(env)
            ch.basic_ack(delivery_tag)
        except send_mailing_consumer.RetryableStatusPublishError as exc:
            log.error("Status publish failed after processing; requeueing: %s", exc)
            ch.basic_nack(delivery_tag, requeue=True)
        except SendGridError as exc:
            log.error("Transient send failure, logging and discarding: %s", exc)
            sendgrid_failures.publish_failure_log(
                ch,
                error_description=f"SendGrid dispatch failed: {exc}",
                related_message_id=env.message_id,
            )
            ch.basic_ack(delivery_tag)
        except RuntimeError as exc:
            log.error("Permanent send failure, discarding: %s", exc)
            ch.basic_ack(delivery_tag)
        except Exception:
            log.exception("Unexpected error handling message; discarding")
            ch.basic_ack(delivery_tag)

    return on_message


def _build_alert_callback(schema: etree.XMLSchema):
    """Build a callback for the flat-<alert> system_alert queue (contract §4).

    The Monitoring→Mailing alert flow uses a flat ``<alert>`` root rather
    than the standard ``<message>`` envelope (sanctioned exception, §4).
    Invalid alerts are logged and acked. SendGrid failures are also logged and
    acked to avoid an infinite requeue loop during provider outages.
    """
    def on_message(ch, method, _properties, body):
        delivery_tag = method.delivery_tag
        try:
            system_alert_consumer.handle(body, schema, channel=ch)
            ch.basic_ack(delivery_tag)
        except system_alert_consumer.AlertValidationError as exc:
            log.warning("Invalid system_alert on %s, flagging: %s", method.routing_key, exc)
            logs.publish_system_error(
                ch,
                error_code=logs.INVALID_XML_FORMAT,
                error_description=f"invalid system_alert on {method.routing_key}: {exc}",
                action="xml_validation",
            )
            ch.basic_ack(delivery_tag)
        except SendGridError as exc:
            log.error("Transient send failure on alert, logging and discarding: %s", exc)
            sendgrid_failures.publish_failure_log(
                ch,
                error_description=f"SendGrid alert dispatch failed: {exc}",
            )
            ch.basic_ack(delivery_tag)
        except Exception:
            log.exception("Unexpected error handling alert; discarding")
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
    # Validate config and load all runtime schemas before taking messages.
    schemas = validate_startup_config()
    system_alert_schema = schemas["system_alert"]
    send_mailing_schema = schemas["send_mailing"]

    # Tracks whether we just recovered from an unplanned disconnect.
    # First pass through the loop is a clean startup (False). Set True
    # by the AMQPConnectionError handler so the next successful connect
    # publishes a contract log.
    recovering_from_outage = False
    outage_started_at: float | None = None

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
            channel.queue_declare(queue=LOG_QUEUE, durable=True)
            channel.queue_declare(queue="crm.incoming", durable=True)

            if recovering_from_outage:
                # We came back online after a broker outage. Surface it to
                # Monitoring before resuming consumption.
                duration_s = (
                    time.monotonic() - outage_started_at
                    if outage_started_at is not None else 0.0
                )
                log.critical(
                    "Recovered from broker outage after %.1fs", duration_s,
                    extra={"action": "broker_connect"},
                )
                logs.publish_system_error(
                    channel,
                    error_code=logs.BROKER_OUTAGE,
                    error_description=(
                        f"mailing service recovered from broker outage "
                        f"after {duration_s:.1f}s"
                    ),
                )
                recovering_from_outage = False
                outage_started_at = None

            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=ALERT_QUEUE,
                on_message_callback=_build_alert_callback(system_alert_schema),
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
            log.error(
                "Broker connection lost: %s. Reconnecting...", exc,
                extra={"action": "broker_disconnect"},
            )
            if not recovering_from_outage:
                # First disconnect of this outage; start the clock so the
                # post-recovery log can report duration.
                outage_started_at = time.monotonic()
            recovering_from_outage = True
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
