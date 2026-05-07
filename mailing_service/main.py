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
from failure_tracker import SlidingWindowFailureTracker
from publishers import system_error
from publishers.logs import RabbitMQLogHandler
from sendgrid_client import SendGridError

ALERT_QUEUE = "monitoring.alerts"
CRM_SEND_MAILING_QUEUE = "crm.to.mailing"
FACTURATIE_SEND_MAILING_QUEUE = "facturatie.to.mailing"
MAILING_ERROR_QUEUE = system_error.ERROR_QUEUE   # "mailing.errors"

CONNECT_RETRIES = 10
CONNECT_DELAY_SECONDS = 2
NACK_BACKOFF_SECONDS = 2

# Escalation thresholds for SendGrid outages. A single 5xx is a normal
# transient failure (handled by nack-requeue); 3+ in 60s is a platform
# problem that Operations needs to know about.
_SENDGRID_FAILURES = SlidingWindowFailureTracker(
    window_seconds=60.0, threshold=3, cooldown_seconds=300.0,
)

log = logging.getLogger("mailing_service")


def _configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _enable_log_queue_handler() -> None:
    """Attach RabbitMQLogHandler to the root logger if LOG_QUEUE_ENABLED.

    Off by default until the platform team's logs.xsd is signed off and
    we've verified the publisher behaves under broker-outage conditions
    in staging. Always runs alongside the default StreamHandler — never
    instead of it.
    """
    if os.getenv("LOG_QUEUE_ENABLED", "false").lower() != "true":
        return
    level_name = os.getenv("LOG_QUEUE_LEVEL", "WARNING").upper()
    level = getattr(logging, level_name, logging.WARNING)
    handler = RabbitMQLogHandler(
        connection_factory=lambda: pika.BlockingConnection(_connection_parameters()),
        level=level,
    )
    logging.getLogger().addHandler(handler)
    log.info("RabbitMQLogHandler attached at level %s", level_name)


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
            log.error(
                "Transient send failure, requeueing: %s", exc,
                extra={"action": "sendgrid_dispatch"},
            )
            if _SENDGRID_FAILURES.record_failure():
                # 3+ failures in 60s — escalate to mailing.errors so
                # Monitoring can flag the SendGrid integration as
                # degraded. The single message keeps requeueing as
                # normal; this is purely a heads-up.
                system_error.publish(
                    ch,
                    error_code=system_error.SENDGRID_UNAVAILABLE,
                    error_description=(
                        "SendGrid 5xx/network failures crossed 3-in-60s "
                        f"threshold (latest: {exc})"
                    ),
                    related_message_id=env.message_id if 'env' in locals() else None,
                )
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

    # Tracks whether we just recovered from an unplanned disconnect.
    # First pass through the loop is a clean startup (False). Set True
    # by the AMQPConnectionError handler so the next successful connect
    # publishes a system_error.
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
            # Queue we own and publish to (errors). crm.incoming is owned
            # by CRM and intentionally NOT declared here.
            channel.queue_declare(queue=MAILING_ERROR_QUEUE, durable=True)

            if recovering_from_outage:
                # We came back online after a broker outage. Surface it to
                # Monitoring before resuming consumption — this is the
                # FATAL-equivalent the plan calls for in §2.4.
                duration_s = (
                    time.monotonic() - outage_started_at
                    if outage_started_at is not None else 0.0
                )
                log.critical(
                    "Recovered from broker outage after %.1fs", duration_s,
                    extra={"action": "broker_connect"},
                )
                system_error.publish(
                    channel,
                    error_code=system_error.BROKER_OUTAGE,
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
            log.error(
                "Broker connection lost: %s. Reconnecting...", exc,
                extra={"action": "broker_disconnect"},
            )
            if not recovering_from_outage:
                # First disconnect of this outage; start the clock so the
                # post-recovery system_error can report duration.
                outage_started_at = time.monotonic()
            recovering_from_outage = True
            try:
                connection.close()
            except Exception:
                pass
            time.sleep(CONNECT_DELAY_SECONDS)


def main() -> None:
    _configure_logging()
    _enable_log_queue_handler()
    try:
        run()
    except Exception:
        log.exception("Fatal error in mailing service")
        sys.exit(1)


if __name__ == "__main__":
    main()
