"""Publish v2.0 ``mailing_status`` responses to the ``crm.incoming`` queue.

Contract section 9.1. CRM consumes ``crm.incoming`` and matches our response
to its original ``send_mailing`` request via the propagated ``correlation_id``.

Requires publisher confirms on the channel (``channel.confirm_delivery()``
called once at startup in main.py): the publish call blocks until the broker
acks the response, so a failure here propagates up and the original inbound
message gets nack-requeued. At-least-once on the response side, no silent
drops.

Initial accounting note (mirrors the plan, will be refined in Phase 4):

* ``status=completed`` → delivered=sent, bounced=0, opened=0
* ``status=failed``    → delivered=0, bounced=sent (best-effort), opened=0
* ``status=partial_failure`` is reserved for when the SendGrid Event Webhook
  lands and we have real per-recipient delivery feedback.
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Literal

import pika
from lxml import etree

log = logging.getLogger(__name__)

CRM_INCOMING_QUEUE = "crm.incoming"

Status = Literal["completed", "partial_failure", "failed"]


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_xml(
    *,
    correlation_id: str | None,
    campaign_id: str,
    subject: str,
    sent: int,
    delivered: int,
    bounced: int,
    opened: int,
    bounced_emails: list[str] | None,
    status: Status,
) -> bytes:
    root = etree.Element("message")

    header = etree.SubElement(root, "header")
    etree.SubElement(header, "message_id").text = str(uuid.uuid4())
    etree.SubElement(header, "timestamp").text = _now_utc()
    etree.SubElement(header, "source").text = "mailing"
    etree.SubElement(header, "type").text = "mailing_status"
    etree.SubElement(header, "version").text = "2.0"
    if correlation_id:
        etree.SubElement(header, "correlation_id").text = correlation_id

    body = etree.SubElement(root, "body")
    etree.SubElement(body, "campaign_id").text = campaign_id
    etree.SubElement(body, "subject").text = subject
    etree.SubElement(body, "sent").text = str(sent)
    etree.SubElement(body, "delivered").text = str(delivered)
    etree.SubElement(body, "bounced").text = str(bounced)
    etree.SubElement(body, "opened").text = str(opened)
    if bounced_emails:
        be_el = etree.SubElement(body, "bounced_emails")
        for email in bounced_emails:
            etree.SubElement(be_el, "email").text = email
    etree.SubElement(body, "status").text = status

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


_SCHEMA: etree.XMLSchema | None = None


def _schema() -> etree.XMLSchema:
    global _SCHEMA
    if _SCHEMA is None:
        path = os.getenv("SCHEMAS_DIR", "/app/schemas") + "/mailing_status.xsd"
        _SCHEMA = etree.XMLSchema(etree.parse(path))
    return _SCHEMA


def publish(
    channel,
    *,
    correlation_id: str | None,
    campaign_id: str,
    subject: str,
    sent: int,
    delivered: int,
    bounced: int,
    opened: int,
    bounced_emails: list[str] | None = None,
    status: Status,
) -> None:
    """Build, self-validate, and publish a ``mailing_status`` envelope.

    Raises if the broker rejects the publish (publisher confirms enabled);
    the consumer then nack-requeues the inbound. Self-validation failures
    raise :class:`RuntimeError` because they indicate a bug in our code,
    not a runtime/transport issue.
    """
    raw = _build_xml(
        correlation_id=correlation_id,
        campaign_id=campaign_id,
        subject=subject,
        sent=sent,
        delivered=delivered,
        bounced=bounced,
        opened=opened,
        bounced_emails=bounced_emails,
        status=status,
    )

    if not _schema().validate(etree.fromstring(raw)):
        # Our own builder produced an invalid envelope — fail loudly so
        # we catch the bug in CI/staging, not in front of CRM.
        raise RuntimeError(
            f"mailing_status self-validation failed: {_schema().error_log}"
        )

    channel.basic_publish(
        exchange="",
        routing_key=CRM_INCOMING_QUEUE,
        body=raw,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.info(
        "Published mailing_status campaign=%s status=%s correlation=%s",
        campaign_id, status, correlation_id,
    )
