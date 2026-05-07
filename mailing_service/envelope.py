"""Parse + validate the v2.0 ``<message><header><body>`` envelope.

Single chokepoint for envelope-format compliance. Every consumer feeds the
raw AMQP body through :func:`parse_and_validate` with the per-flow XSD it
expects. Failures are raised as :class:`MalformedXMLError` or
:class:`SchemaValidationError`; the consumer translates either one into a
``system_error`` publish + ack (see ``publishers/system_error.py``).
"""

from dataclasses import dataclass

from lxml import etree

# huge_tree=True lets us parse send_mailing payloads with large base64
# attachments (the consumer's MAX_ATTACHMENT_BYTES guard rejects oversized
# ones AFTER the parse succeeds — we can't size-check what we can't read).
# Safe here because messages come from trusted internal services; we don't
# expand entities or follow external references.
_PARSER = etree.XMLParser(huge_tree=True)


class MalformedXMLError(Exception):
    """Raised when the bytes cannot be parsed as XML at all."""


class SchemaValidationError(Exception):
    """Raised when the XML parses but does not conform to the supplied XSD."""


@dataclass(frozen=True)
class Envelope:
    """Header fields plus the ``<body>`` element of a v2.0 message.

    ``body`` is the raw lxml element so each consumer can extract its own
    flow-specific fields without re-parsing.
    """

    message_id: str
    correlation_id: str | None
    source: str
    type: str
    timestamp: str
    body: etree._Element


def parse_and_validate(raw: bytes, schema: etree.XMLSchema) -> Envelope:
    """Parse ``raw`` and validate against ``schema``; return the envelope.

    Raises :class:`MalformedXMLError` on syntactically invalid XML and
    :class:`SchemaValidationError` when the document does not conform to
    the supplied schema.
    """
    if len(raw) > 50 * 1024 * 1024:  # 50 MB safety limit
        raise MalformedXMLError(f"Payload size {len(raw)} exceeds safety limit")

    try:
        doc = etree.fromstring(raw, _PARSER)
    except etree.XMLSyntaxError as exc:
        raise MalformedXMLError(str(exc)) from exc

    if not schema.validate(doc):
        raise SchemaValidationError(str(schema.error_log))

    header = doc.find("header")
    body = doc.find("body")
    if header is None or body is None:
        # Should be impossible after schema.validate() succeeds, but defend
        # against future XSD changes that loosen the structure.
        raise SchemaValidationError("Envelope missing <header> or <body>")

    return Envelope(
        message_id=header.findtext("message_id", default=""),
        correlation_id=header.findtext("correlation_id"),
        source=header.findtext("source", default=""),
        type=header.findtext("type", default=""),
        timestamp=header.findtext("timestamp", default=""),
        body=body,
    )
