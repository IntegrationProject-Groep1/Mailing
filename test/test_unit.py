import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = ROOT / "mailing_service" / "schemas"
sys.path.insert(0, str(ROOT / "mailing_service"))

import envelope
import main
import sendgrid_client
import sendgrid_failures
from consumers import send_mailing, system_alert
from publishers import logs, mailing_status


class FakeMethod:
    delivery_tag = 123
    routing_key = "monitoring.alerts"


class FakeChannel:
    def __init__(self):
        self.published = []
        self.acks = []
        self.nacks = []

    def basic_publish(self, *, exchange, routing_key, body, properties):
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))


class FailingOnceStatusChannel(FakeChannel):
    def __init__(self):
        super().__init__()
        self.failed_once = False

    def basic_publish(self, *, exchange, routing_key, body, properties):
        if routing_key == "crm.incoming" and not self.failed_once:
            self.failed_once = True
            raise RuntimeError("broker publish failed")
        super().basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
        )


def _schema(name: str) -> etree.XMLSchema:
    return etree.XMLSchema(etree.parse(str(SCHEMAS_DIR / f"{name}.xsd")))


def _send_mailing_env() -> envelope.Envelope:
    raw = b"""<?xml version="1.0" encoding="UTF-8"?>
<message>
  <header>
    <message_id>11111111-1111-4111-8111-111111111111</message_id>
    <timestamp>2026-05-15T10:00:00Z</timestamp>
    <source>crm</source>
    <type>send_mailing</type>
    <version>2.0</version>
    <correlation_id>22222222-2222-4222-8222-222222222222</correlation_id>
  </header>
  <body>
    <campaign_id>campaign-1</campaign_id>
    <subject>Subject</subject>
    <mail_type>registration_confirmation</mail_type>
    <recipients>
      <recipient>
        <email>jan@example.test</email>
        <identity_uuid>33333333-3333-4333-8333-333333333333</identity_uuid>
        <contact>
          <first_name>Jan</first_name>
          <last_name>Peeters</last_name>
        </contact>
      </recipient>
    </recipients>
    <template_data>{"session_title":"Talk","session_date":"2026-05-15"}</template_data>
  </body>
</message>"""
    return envelope.parse_and_validate(raw, _schema("send_mailing"))


def _body_for(published, routing_key: str) -> etree._Element:
    for item in published:
        if item["routing_key"] == routing_key:
            return etree.fromstring(item["body"])
    raise AssertionError(f"no publish to {routing_key}")


class MailingServiceUnitTests(unittest.TestCase):
    def setUp(self):
        os.environ["SCHEMAS_DIR"] = str(SCHEMAS_DIR)
        mailing_status._SCHEMA = None
        logs._SCHEMA = None
        send_mailing.reset_idempotency_state()
        sendgrid_failures.reset_failure_tracker()

    def test_mailing_status_with_bounced_emails_validates(self):
        root = mailing_status._build_element(
            correlation_id="22222222-2222-4222-8222-222222222222",
            campaign_id="campaign-1",
            subject="Subject",
            sent=1,
            delivered=0,
            bounced=1,
            opened=0,
            bounced_emails=["bad@example.test"],
            status="failed",
        )

        self.assertTrue(mailing_status._schema().validate(root), mailing_status._schema().error_log)
        body_order = [child.tag for child in root.find("body")]
        self.assertLess(body_order.index("bounced_emails"), body_order.index("opened"))

    def test_log_publisher_builds_contract_valid_xml(self):
        root = logs._build_element(
            level="error",
            action="system_error",
            message="error_code=invalid_xml_format; description=test",
        )

        self.assertTrue(logs._schema().validate(root), logs._schema().error_log)
        self.assertEqual(root.findtext("header/source"), "mailing")
        self.assertEqual(root.findtext("header/type"), "log")

    def test_invalid_flat_alert_publishes_log_and_acks(self):
        callback = main._build_alert_callback(_schema("system_alert"))
        channel = FakeChannel()

        callback(channel, FakeMethod(), None, b"<alert><system>facturatie</system></alert>")

        self.assertEqual(channel.acks, [FakeMethod.delivery_tag])
        self.assertEqual(channel.nacks, [])
        root = _body_for(channel.published, "logs")
        self.assertTrue(logs._schema().validate(root), logs._schema().error_log)
        self.assertEqual(root.findtext("body/action"), "xml_validation")
        self.assertIn("invalid_xml_format", root.findtext("body/message"))

    def test_alert_fields_are_escaped_before_html_send(self):
        raw = b"""<?xml version="1.0" encoding="UTF-8"?>
<alert>
  <type>HEARTBEAT_CRITICAL</type>
  <system>&lt;b&gt;facturatie&lt;/b&gt;</system>
  <message>&lt;script&gt;alert(1)&lt;/script&gt;</message>
  <timestamp>2026-05-15T10:00:00Z</timestamp>
</alert>"""

        with patch("sendgrid_client.send_email") as send_email:
            system_alert.handle(raw, _schema("system_alert"))

        html_body = send_email.call_args.args[1]
        self.assertIn("&lt;b&gt;facturatie&lt;/b&gt;", html_body)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html_body)
        self.assertNotIn("<script>alert(1)</script>", html_body)

    def test_successful_system_alert_publishes_log(self):
        raw = b"""<?xml version="1.0" encoding="UTF-8"?>
<alert>
  <type>HEARTBEAT_CRITICAL</type>
  <system>facturatie</system>
  <message>Service is down</message>
  <timestamp>2026-05-15T10:00:00Z</timestamp>
</alert>"""
        channel = FakeChannel()
        with patch("sendgrid_client.send_email"):
            system_alert.handle(raw, _schema("system_alert"), channel=channel)

        log_root = _body_for(channel.published, "logs")
        self.assertEqual(log_root.findtext("body/level"), "info")
        self.assertEqual(log_root.findtext("body/action"), "email")
        self.assertIn("Successfully sent system alert email for system=facturatie", log_root.findtext("body/message"))

    def test_sendgrid_failure_logs_and_publishes_failed_status(self):
        env = _send_mailing_env()
        channel = FakeChannel()

        with patch.dict(os.environ, {"SCHEMAS_DIR": str(SCHEMAS_DIR), "FROM_EMAIL": "from@example.test"}, clear=True):
            with patch("templates.resolve_template_id", return_value="d-template"):
                with patch(
                    "sendgrid_client.send_template_email",
                    side_effect=sendgrid_client.SendGridError("provider down"),
                ):
                    send_mailing.handle(env, channel)

        status = _body_for(channel.published, "crm.incoming")
        log_root = _body_for(channel.published, "logs")
        self.assertEqual(status.findtext("body/status"), "failed")
        self.assertEqual(log_root.findtext("body/action"), "email")
        self.assertIn("sendgrid_unavailable", log_root.findtext("body/message"))

    def test_successful_send_mailing_publishes_log(self):
        env = _send_mailing_env()
        channel = FakeChannel()

        with patch.dict(os.environ, {"SCHEMAS_DIR": str(SCHEMAS_DIR), "FROM_EMAIL": "from@example.test"}, clear=True):
            with patch("templates.resolve_template_id", return_value="d-template"):
                with patch(
                    "sendgrid_client.send_template_email",
                    return_value=sendgrid_client.SendResult(accepted=["jan@example.test"]),
                ):
                    send_mailing.handle(env, channel)

        log_root = _body_for(channel.published, "logs")
        self.assertEqual(log_root.findtext("body/level"), "info")
        self.assertEqual(log_root.findtext("body/action"), "email")
        self.assertIn("Successfully sent email campaign=campaign-1 to 1 recipients", log_root.findtext("body/message"))

    def test_duplicate_completed_send_mailing_does_not_resend(self):
        env = _send_mailing_env()
        channel = FakeChannel()

        with patch.dict(os.environ, {"SCHEMAS_DIR": str(SCHEMAS_DIR), "FROM_EMAIL": "from@example.test"}, clear=True):
            with patch("templates.resolve_template_id", return_value="d-template"):
                with patch(
                    "sendgrid_client.send_template_email",
                    return_value=sendgrid_client.SendResult(accepted=["jan@example.test"]),
                ) as send_template:
                    send_mailing.handle(env, channel)
                    send_mailing.handle(env, channel)

        self.assertEqual(send_template.call_count, 1)
        self.assertEqual(
            [item["routing_key"] for item in channel.published].count("crm.incoming"),
            1,
        )

    def test_pending_status_redelivery_does_not_resend(self):
        env = _send_mailing_env()
        channel = FailingOnceStatusChannel()

        with patch.dict(os.environ, {"SCHEMAS_DIR": str(SCHEMAS_DIR), "FROM_EMAIL": "from@example.test"}, clear=True):
            with patch("templates.resolve_template_id", return_value="d-template"):
                with patch(
                    "sendgrid_client.send_template_email",
                    return_value=sendgrid_client.SendResult(accepted=["jan@example.test"]),
                ) as send_template:
                    with self.assertRaises(send_mailing.RetryableStatusPublishError):
                        send_mailing.handle(env, channel)
                    send_mailing.handle(env, channel)

        self.assertEqual(send_template.call_count, 1)
        self.assertEqual(
            [item["routing_key"] for item in channel.published].count("crm.incoming"),
            1,
        )

    def test_startup_validation_requires_env(self):
        with patch.dict(os.environ, {"SCHEMAS_DIR": str(SCHEMAS_DIR)}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "Missing required environment"):
                main.validate_startup_config()

    def test_startup_validation_passes_with_complete_env(self):
        env = {
            "SCHEMAS_DIR": str(SCHEMAS_DIR),
            "RABBITMQ_HOST": "rabbitmq",
            "RABBITMQ_USER": "mailing",
            "RABBITMQ_PASS": "mailing",
            "SENDGRID_API_KEY": "SG.test",
            "FROM_EMAIL": "from@example.test",
            "ADMIN_EMAILS": "admin@example.test",
            "SENDGRID_TEMPLATE_REGISTRATION_CONFIRMATION": "d-registration",
            "SENDGRID_TEMPLATE_PAYMENT_CONFIRMATION": "d-payment",
            "SENDGRID_TEMPLATE_INVOICE_READY": "d-invoice",
            "SENDGRID_TEMPLATE_SESSION_UPDATE": "d-session",
            "SENDGRID_TEMPLATE_GENERAL_ANNOUNCEMENT": "d-general",
        }
        with patch.dict(os.environ, env, clear=True):
            schemas = main.validate_startup_config()

        self.assertEqual(set(schemas), {"system_alert", "send_mailing", "mailing_status", "log"})


if __name__ == "__main__":
    unittest.main()
