"""Publish contract ``log`` messages to Monitoring's ``logs`` queue."""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Literal

import pika
from lxml import etree

log = logging.getLogger(__name__)

LOG_QUEUE = "logs"

Level = Literal["info", "warning", "error"]
Action = Literal[
    "registration",
    "user",
    "payment",
    "invoice",
    "session",
    "calendar",
    "email",
    "wallet",
    "refund",
    "identity",
    "xml_validation",
    "system_error",
    "badge",
]

INVALID_XML_FORMAT = "invalid_xml_format"
UNKNOWN_MESSAGE_TYPE = "unknown_message_type"
SENDGRID_UNAVAILABLE = "sendgrid_unavailable"
BROKER_OUTAGE = "broker_outage"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_element(*, level: Level, action: Action, message: str) -> etree._Element:
    root = etree.Element("message")

    header = etree.SubElement(root, "header")
    etree.SubElement(header, "message_id").text = str(uuid.uuid4())
    etree.SubElement(header, "timestamp").text = _now_utc()
    etree.SubElement(header, "source").text = "mailing"
    etree.SubElement(header, "type").text = "log"
    etree.SubElement(header, "version").text = "2.0"

    body = etree.SubElement(root, "body")
    etree.SubElement(body, "level").text = level
    etree.SubElement(body, "action").text = action
    etree.SubElement(body, "message").text = message

    return root


_SCHEMA: etree.XMLSchema | None = None


def _schema() -> etree.XMLSchema:
    global _SCHEMA
    if _SCHEMA is None:
        path = os.getenv("SCHEMAS_DIR", "/app/schemas") + "/log.xsd"
        _SCHEMA = etree.XMLSchema(etree.parse(path))
    return _SCHEMA


def publish(channel, *, level: Level, action: Action, message: str) -> bool:
    """Build, self-validate, and publish one contract ``log`` message.

    Returns ``True`` when the publish call succeeds. Returns ``False`` after
    logging local failures so error-reporting problems do not crash the
    consumer path that tried to report them.
    """
    root = _build_element(level=level, action=action, message=message)

    try:
        if not _schema().validate(root):
            log.error("log self-validation failed; not publishing: %s", _schema().error_log)
            return False
    except Exception:
        log.exception("log self-validation crashed; not publishing")
        return False

    try:
        channel.basic_publish(
            exchange="",
            routing_key=LOG_QUEUE,
            body=etree.tostring(root, xml_declaration=True, encoding="UTF-8"),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        log.info("Published log level=%s action=%s", level, action)
        return True
    except Exception:
        log.exception("Failed to publish log to %s", LOG_QUEUE)
        return False


def publish_system_error(
    channel,
    *,
    error_code: str,
    error_description: str,
    related_message_id: str | None = None,
    action: Action = "system_error",
) -> bool:
    """Publish a system-error-shaped event using the contract ``log`` format."""
    parts = [f"error_code={error_code}", f"description={error_description}"]
    if related_message_id:
        parts.append(f"related_message_id={related_message_id}")
    return publish(channel, level="error", action=action, message="; ".join(parts))
