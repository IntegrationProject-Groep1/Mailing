# Mailing Service

Python service that consumes alert XML messages from the shared platform RabbitMQ broker and sends e-mails via SendGrid. Part of the Desideriushogeschool event-management platform.

## Current scope

Only the monitoring alert flow: when the monitoring team publishes a `<alert>` to the `monitoring.alerts` queue, this service validates it against `alert.xsd` and e-mails the admin list (red for `down`, green for `up`).

```xml
<alert>
  <system>facturatie</system>
  <status>down</status>
  <timestamp>2026-03-15T10:35:12Z</timestamp>
</alert>
```

## Layout

```
mailing/
├── docker-compose.yml        mailing_service deployment
├── .env.example              copy to .env and fill in
├── alert.xsd                 XSD used for message validation
├── mailing_service/          the service itself
└── test/                     local-only test harness (RabbitMQ + publisher)
```

## Running test locally

1. `cp .env.example .env` and fill in a real `SENDGRID_API_KEY`, a SendGrid-verified `FROM_EMAIL`, and `ADMIN_EMAILS`.
2. Start the local test broker:
   ```
   cd test && docker compose up -d
   ```
3. Start the mailing service:
   ```
   cd .. && docker compose up -d --build
   ```
4. Publish a fake alert (from the host):
   ```
   cd test
   pip install -r requirements.txt
   python test_alerts.py down     # or: up, malformed, all
   ```

The RabbitMQ management UI is at http://localhost:15672 (user `mailing`, pass `mailing`).

## Stack

Python 3.12, pika, lxml, sendgrid. RabbitMQ 3.13-management for local testing.
