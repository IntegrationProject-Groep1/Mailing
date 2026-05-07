"""Publish v2.0 ``system_error`` messages to the ``mailing.errors`` queue.

Contract section 2.6 defines the global error format. We publish here whenever
an inbound message is malformed, fails XSD validation, references an unknown
mail_type, or trips the attachment size guard. Monitoring's Logstash pipeline
consumes ``mailing.errors`` and surfaces these on the dashboard, so this is
how contract drift becomes visible to other teams in real time.
"""

import logging
import os
import uuid
from datetime import datetime, timezone

import pika
from lxml import etree

log = logging.getLogger(__name__)

ERROR_QUEUE = "mailing.errors"

# Error codes shared across teams (contract §2.6) plus a few flow-specific
# codes used only by Mailing. Constants exist so callers don't fat-finger
# the wire string.
INVALID_XML_FORMAT = "invalid_xml_format"
UNKNOWN_MESSAGE_TYPE = "unknown_message_type"
# Phase 2 escalations — surface platform-level outages so Operations can react.
SENDGRID_UNAVAILABLE = "sendgrid_unavailable"
BROKER_OUTAGE = "broker_outage"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_element(error_code: str, error_description: str, related_message_id: str | None) -> etree._Element:
    root = etree.Element("message")
    header = etree.SubElement(root, "header")
    etree.SubElement(header, "message_id").text = str(uuid.uuid4())
    etree.SubElement(header, "type").text = "system_error"
    etree.SubElement(header, "source").text = "mailing"
    etree.SubElement(header, "timestamp").text = _now_utc()
    etree.SubElement(header, "version").text = "2.0"

    body = etree.SubElement(root, "body")
    etree.SubElement(body, "error_code").text = error_code
    etree.SubElement(body, "error_description").text = error_description
    if related_message_id:
        etree.SubElement(body, "related_message_id").text = related_message_id

    return root


def _load_schema() -> etree.XMLSchema:
    path = os.getenv("SCHEMAS_DIR", "/app/schemas") + "/system_error.xsd"
    return etree.XMLSchema(etree.parse(path))


_SCHEMA: etree.XMLSchema | None = None


def _schema() -> etree.XMLSchema:
    """Lazy-load the schema so importing this module does not require the file."""
    global _SCHEMA
    if _SCHEMA is None:
        _SCHEMA = _load_schema()
    return _SCHEMA


def publish(
    channel,
    *,
    error_code: str,
    error_description: str,
    related_message_id: str | None = None,
) -> None:
    """Publish a ``system_error`` message to the ``mailing.errors`` queue.

    Self-validates against ``system_error.xsd`` before publishing — any
    failure here means our own builder is broken, not the caller. We log
    and swallow rather than raise: a failed error-publish must not also
    take down the consumer that triggered it.
    """
    root = _build_element(error_code, error_description, related_message_id)

    # Self-validation: the schema constrains source/type/version, so this
    # catches accidental drift in our own code path (e.g., enum changes).
    try:
        if not _schema().validate(root):
            log.error(
                "system_error self-validation failed; not publishing: %s",
                _schema().error_log,
            )
            return
    except Exception:
        log.exception("system_error self-validation crashed; not publishing")
        return

    try:
        channel.basic_publish(
            exchange="",
            routing_key=ERROR_QUEUE,
            body=etree.tostring(root, xml_declaration=True, encoding="UTF-8"),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        log.warning(
            "Published system_error code=%s related=%s",
            error_code, related_message_id,
        )
    except Exception:
        # Broker hiccup while reporting an error is unfortunate but must
        # not crash the consumer. The original message will be acked
        # normally; the operator just won't see it on the dashboard.
        log.exception("Failed to publish system_error to %s", ERROR_QUEUE)
