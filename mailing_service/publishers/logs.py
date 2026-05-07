"""Forward Python ``logging`` records to the shared ``logs`` queue.

A logging.Handler subclass that serialises each LogRecord into the v2.0
log envelope (``schemas/logs.xsd``) and publishes it to RabbitMQ. Three
guarantees, in priority order:

1. **emit() never blocks the caller.** Records go into a bounded in-memory
   queue via ``put_nowait``. If the queue is full (broker outage or backed-up
   publish thread), the oldest record is dropped and a rate-limited warning
   is written to stderr. The mailing service's request path must NOT slow
   down because of log-pipeline issues.

2. **Dedicated pika connection.** The publish thread owns its own broker
   connection — never share with the main consumer thread. Sharing would
   risk reentrancy if a delivery callback ever logs from inside pika's
   I/O loop.

3. **stdout still works.** The handler runs ALONGSIDE the default
   StreamHandler, never instead of it. ``kubectl logs`` always shows the
   same records the queue receives.

Adding ``action`` and ``correlation_id`` from a log call::

    log.info("Dispatching campaign", extra={
        "action": "sendgrid_dispatch",
        "correlation_id": env.correlation_id,
        "context": {"campaign_id": campaign_id, "recipient_count": 42},
    })

Records without an ``action`` extra get a synthesised default from the
logger name's last component.
"""

import logging
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import pika
from lxml import etree

LOG_QUEUE = "logs"
DEFAULT_BUFFER_SIZE = 1000

# Map Python logging levels → contract enum {INFO, WARN, ERROR, FATAL}.
_LEVEL_MAP = {
    logging.DEBUG:    "INFO",
    logging.INFO:     "INFO",
    logging.WARNING:  "WARN",
    logging.ERROR:    "ERROR",
    logging.CRITICAL: "FATAL",
}

# Don't recurse: any record produced by this module is dropped before emit.
_OWN_LOGGER_PREFIX = "mailing_service.publishers.logs"

# Throttle the stderr "queue full" warning: at most once per 60s so a
# sustained outage doesn't drown kubectl logs.
_WARN_INTERVAL_SECONDS = 60


def _now_utc(epoch: float) -> str:
    """Format a logging.LogRecord.created (epoch float) as ISO-8601 UTC."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _record_to_xml(record: logging.LogRecord) -> bytes:
    """Serialise one LogRecord to a logs.xsd-conformant message."""
    root = etree.Element("message")

    header = etree.SubElement(root, "header")
    etree.SubElement(header, "message_id").text = str(uuid.uuid4())
    etree.SubElement(header, "timestamp").text = _now_utc(record.created)
    etree.SubElement(header, "source").text = "mailing"
    etree.SubElement(header, "type").text = "log"
    etree.SubElement(header, "version").text = "2.0"
    correlation_id = getattr(record, "correlation_id", None)
    if correlation_id:
        etree.SubElement(header, "correlation_id").text = str(correlation_id)

    body = etree.SubElement(root, "body")
    etree.SubElement(body, "level").text = _LEVEL_MAP.get(record.levelno, "INFO")
    # action: prefer the extra={"action": "..."}; fall back to the last
    # dotted segment of the logger name.
    action = getattr(record, "action", None) or record.name.rsplit(".", 1)[-1]
    etree.SubElement(body, "action").text = str(action)
    etree.SubElement(body, "logger").text = record.name
    msg = record.getMessage()
    if record.exc_info:
        # Append stack trace to message; logs.xsd allows arbitrary string.
        msg = f"{msg}\n{logging.Formatter().formatException(record.exc_info)}"
    etree.SubElement(body, "message").text = msg

    context = getattr(record, "context", None)
    if context:
        ctx_el = etree.SubElement(body, "context")
        for k, v in context.items():
            entry = etree.SubElement(ctx_el, "entry", key=str(k))
            entry.text = str(v)

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


class RabbitMQLogHandler(logging.Handler):
    """Logging handler that forwards records to the ``logs`` queue."""

    def __init__(
        self,
        connection_factory,
        *,
        queue_name: str = LOG_QUEUE,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        level: int = logging.WARNING,
    ) -> None:
        """Construct the handler.

        ``connection_factory`` is a zero-arg callable returning a fresh
        ``pika.BlockingConnection``. Injecting a factory rather than the
        connection params themselves keeps the handler unaware of the
        env-var conventions and trivially testable.
        """
        super().__init__(level=level)
        self._queue_name = queue_name
        self._buffer: queue.Queue[bytes] = queue.Queue(maxsize=buffer_size)
        self._connection_factory = connection_factory
        self._stop_event = threading.Event()
        self._last_full_warn = 0.0
        self._thread = threading.Thread(
            target=self._drain_loop,
            name="RabbitMQLogHandler",
            daemon=True,
        )
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        """Serialise + enqueue. Non-blocking; drops on full."""
        if threading.current_thread() == self._thread:
            # Prevent recursion: any logs generated by this thread (including
            # from pika or lxml) must not feed back into the queue.
            return
        try:
            payload = _record_to_xml(record)
        except Exception:
            # Serialisation failure must NEVER crash the caller. Best
            # effort: skip this record entirely.
            return

        try:
            self._buffer.put_nowait(payload)
        except queue.Full:
            self._on_buffer_full()

    def _on_buffer_full(self) -> None:
        """Rate-limited stderr warning when the buffer drops a record."""
        now = time.monotonic()
        if now - self._last_full_warn < _WARN_INTERVAL_SECONDS:
            return
        self._last_full_warn = now
        sys.stderr.write(
            f"[RabbitMQLogHandler] log buffer full ({self._buffer.maxsize}); "
            f"dropping records (this warning is rate-limited to once per "
            f"{_WARN_INTERVAL_SECONDS}s)\n",
        )
        sys.stderr.flush()

    def close(self) -> None:
        self._stop_event.set()
        super().close()

    def _drain_loop(self) -> None:
        """Background thread: open broker connection, drain buffer, reconnect on failure."""
        connection = None
        channel = None
        while not self._stop_event.is_set():
            try:
                if connection is None:
                    connection = self._connection_factory()
                    channel = connection.channel()
                    channel.queue_declare(queue=self._queue_name, durable=True)

                # Block up to 1s waiting for a record so stop_event is responsive.
                try:
                    payload = self._buffer.get(timeout=1.0)
                except queue.Empty:
                    continue

                try:
                    channel.basic_publish(
                        exchange="",
                        routing_key=self._queue_name,
                        body=payload,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                except Exception:
                    # Re-queue the payload at the front and reconnect.
                    # put_nowait may itself fail (buffer full again) — in
                    # that case the record is lost; we already emitted a
                    # rate-limited warning.
                    try:
                        self._buffer.put_nowait(payload)
                    except queue.Full:
                        pass
                    raise
            except Exception:
                # Any error: tear down the connection, sleep, retry.
                try:
                    if connection is not None:
                        connection.close()
                except Exception:
                    pass
                connection = None
                channel = None
                time.sleep(2.0)
