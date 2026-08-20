# V1 actionable alerting

Prometheus loads docker/prometheus-alerts.yml and routes alerts to Alertmanager.
Alertmanager groups and deduplicates alerts, sends both firing and resolved
notifications to Telegram, and is published only on loopback port 9093.

## Secrets

Alertmanager does not contain Telegram credentials in Compose, YAML, Git, logs,
or command arguments. Before starting the observability profile, create:

- secrets/telegram-bot-token: the bot token only;
- secrets/telegram-chat-id: the numeric chat ID only.

On Linux, make both files readable only by the Alertmanager container identity
UID/GID 65534, and keep the secrets directory outside image build context.
ALERTMANAGER_TELEGRAM_BOT_TOKEN_FILE and
ALERTMANAGER_TELEGRAM_CHAT_ID_FILE may point to another protected host path.

## Alert contract

Rules cover stale or missing streams, gap/checksum/invalid data, P99 order and
runner latency, reject bursts, unknown order outcomes, exposure and drawdown
limit utilization, reconciliation failures/drift, runner stalls, and the live
kill switch. Every rule includes severity, impact, immediate action, and runbook
location. Critical alerts require entries to remain disabled.

## Linux preflight

Before any deployment, run the exact container versions from Compose to validate
configuration:

1. promtool check rules /etc/prometheus/rules/prometheus-alerts.yml
2. promtool check config /etc/prometheus/prometheus.yml
3. amtool check-config /etc/alertmanager/alertmanager.yml
4. Start the observability profile with dummy non-production Telegram secret
   files, verify Prometheus sees Alertmanager, and fire one synthetic test alert.
5. Replace dummy files with protected operator credentials and verify one firing
   plus one resolved Telegram notification.

This preflight is not claimed locally because Docker is unavailable on this
Windows workstation.