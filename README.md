# Mailing Service

Python service that consumes v2.0 platform-contract messages from the shared RabbitMQ broker and sends e-mails via SendGrid. Part of the Desideriushogeschool event-management platform.

## Flows

| # | Source | Inbound queue | Outbound | Purpose |
|---|---|---|---|---|
| 1 | Monitoring | `monitoring.alerts` | — | Admin email on system online/offline (contract §4) |
| 2 | CRM | `crm.to.mailing` | `crm.incoming` (`mailing_status`) | Transactional email via SendGrid Dynamic Template (contract §12.1) |
| 3 | Facturatie | `facturatie.to.mailing` | `crm.incoming` (`mailing_status`) | Same template flow as CRM, different source (contract §13.1) |

Every inbound message that fails XML parsing or schema validation, plus a few permanent application-level failures, also produces a contract `log` message on `logs` so Monitoring sees contract drift in real time. Two additional escalations fire automatically:

- **`sendgrid_unavailable`** for SendGrid 5xx/network failures. Every failed send is logged and converted to failed status where possible; 3+ failures within 60 s also emit a rate-limited outage-level log.
- **`broker_outage`** the first time we successfully reconnect after losing the broker connection. Includes the outage duration in the log message.

SendGrid outages are not nack/requeued. This avoids an infinite requeue loop while the provider is down. The service keeps in-memory idempotency state for processed `send_mailing` message IDs and pending `mailing_status` publishes, but that state is lost on process restart.

Heartbeats are out of scope for this repo (Sidecar Principle, contract §3.1) — the platform's deployment workflow attaches the heartbeat sidecar; nothing in this codebase or its `docker-compose.yml` references it.

## Message shape

`send_mailing`, `mailing_status`, and `log` use the v2.0 envelope:

```xml
<message>
  <header>
    <message_id>...</message_id>
    <timestamp>2026-04-24T10:35:12Z</timestamp>
    <source>crm|facturatie|mailing</source>
    <type>send_mailing|mailing_status|log</type>
    <version>2.0</version>
    <correlation_id>...</correlation_id>   <!-- mandatory for send_mailing -->
  </header>
  <body>
    <!-- per-flow fields; see mailing_service/schemas/*.xsd -->
  </body>
</message>
```

Monitoring alerts are the contract's sanctioned exception: they use a flat `<alert>` root and are consumed from `monitoring.alerts`.

Authoritative runtime schemas live in [mailing_service/schemas/](mailing_service/schemas/) and are validated on every inbound message and every outbound `mailing_status` / `log`. The root-level `xsd/` copies were removed to avoid duplicate schema sources.

## SendGrid templates

The `send_mailing` flow uses SendGrid Dynamic Templates. Each contract `mail_type` maps to one SendGrid template ID via an env var:

| `mail_type` | Env var | Expected template variables |
|---|---|---|
| `registration_confirmation` | `SENDGRID_TEMPLATE_REGISTRATION_CONFIRMATION` | `session_title`, `session_date` |
| `payment_confirmation` | `SENDGRID_TEMPLATE_PAYMENT_CONFIRMATION` | `amount`, `currency`, `paid_at` |
| `invoice_ready` | `SENDGRID_TEMPLATE_INVOICE_READY` | `invoice_id`, `amount`, `currency`, `due_date` |
| `session_update` | `SENDGRID_TEMPLATE_SESSION_UPDATE` | `session_title`, `change_reason` |
| `general_announcement` | `SENDGRID_TEMPLATE_GENERAL_ANNOUNCEMENT` | (campaign-specific) |

Adding a new mail_type: extend the `MailType` enum in [mailing_service/templates.py](mailing_service/templates.py), update the `mail_type` enum in [mailing_service/schemas/send_mailing.xsd](mailing_service/schemas/send_mailing.xsd), and set the corresponding env var.

## Layout

```
mailing/
├── docker-compose.yml          mailing_service deployment
├── Dockerfile                  mailing_service image build
├── .env.example                copy to .env and fill in
├── mailing_service/
│   ├── main.py                 connection lifecycle + 3× basic_consume
│   ├── envelope.py             v2.0 envelope parse + validate helper
│   ├── consumers/              per-flow handlers (system_alert, send_mailing)
│   ├── publishers/             outbound publishers (mailing_status, logs)
│   ├── sendgrid_client.py      SendGrid wrapper (plain + template variants)
│   ├── templates.py            mail_type → SendGrid template_id mapping
│   └── schemas/                v2.0 XSDs
└── test/                       local-only test harness (RabbitMQ + publisher)
    ├── docker-compose.yml      test broker
    ├── test_messages.py        scenario publisher + response-queue drain
    └── fixtures/               per-scenario XML payloads
```

## Running locally

1. `cp .env.example .env` and fill in a real `SENDGRID_API_KEY`, a SendGrid-verified `FROM_EMAIL`, `ADMIN_EMAILS`, and the five `SENDGRID_TEMPLATE_*` IDs. The service validates these variables and all runtime schemas before it starts consuming.
2. Start the test broker:
   ```
   cd test && docker compose up -d
   ```
3. Start the mailing service:
   ```
   cd .. && docker compose up -d --build
   ```
4. Publish messages and watch responses:
   ```
   cd test
   pip install -r requirements.txt
   python test_messages.py --scenario all --listen-mailing-status --listen-logs
   ```

The RabbitMQ management UI is at http://localhost:15672 (user `mailing`, pass `mailing`).

Run unit checks locally with:
```
SCHEMAS_DIR=$PWD/mailing_service/schemas python -m unittest discover -s test -p "test_*.py"
```

### Available scenarios

`system_alert_offline`, `system_alert_online`, `legacy_alert` (verifies strict-mode rejection), `malformed_envelope_missing_header`, `crm_send_mailing`, `crm_send_mailing_attachment`, `crm_send_mailing_oversized_attachment` (verifies the 25 MB guard), `crm_send_mailing_unknown_type`, `facturatie_send_mailing`, `all`.

## Stack

Python 3.12, pika, lxml, sendgrid. RabbitMQ 3.13-management for local testing.
