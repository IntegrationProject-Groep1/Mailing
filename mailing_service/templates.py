"""Map a contract ``mail_type`` to a SendGrid Dynamic Template ID.

Template IDs are environment-specific (different SendGrid accounts in dev
vs prod) so we resolve through env vars rather than hardcoding. The enum
prevents the "we silently accepted an unknown type" failure mode that a
plain ``dict.get`` would allow.

Adding a new mail_type:
  1. Add the enum value (must match the contract's snake_case wire string).
  2. Set ``SENDGRID_TEMPLATE_<NAME>`` in ``.env`` / k8s ConfigMap.
  3. Update the send_mailing.xsd ``mail_type`` enum to mirror.
"""

import os
from enum import StrEnum


_ENV_PREFIX = "SENDGRID_TEMPLATE_"


class MailType(StrEnum):
    """Mail-type strings as they appear on the wire (contract §12.1)."""

    REGISTRATION_CONFIRMATION = "registration_confirmation"
    PAYMENT_CONFIRMATION = "payment_confirmation"
    INVOICE_READY = "invoice_ready"
    SESSION_UPDATE = "session_update"
    GENERAL_ANNOUNCEMENT = "general_announcement"
    # Monitoring → Mailing daily platform report (queue monitoring.reports,
    # XSD monitoring_report.xsd). Distinct wire shape from CRM/Facturatie's
    # send_mailing.xsd; the enum value is shared.
    DAILY_REPORT = "daily_report"


class UnknownMailTypeError(Exception):
    """Raised when an inbound ``mail_type`` is not in :class:`MailType`."""


class MissingTemplateError(Exception):
    """Raised when the env var for a known mail_type is unset / empty."""


def resolve_template_id(mail_type: str) -> str:
    """Return the SendGrid template_id for ``mail_type``.

    Raises :class:`UnknownMailTypeError` if ``mail_type`` isn't in the
    enum (contract drift) or :class:`MissingTemplateError` if it's known
    but the corresponding env var is unset (deployment misconfiguration).
    """
    try:
        mt = MailType(mail_type)
    except ValueError as exc:
        raise UnknownMailTypeError(mail_type) from exc

    env_name = f"{_ENV_PREFIX}{mt.name}"
    template_id = os.environ.get(env_name)
    if not template_id:
        raise MissingTemplateError(f"{env_name} is unset; cannot send {mt.value}")
    return template_id
