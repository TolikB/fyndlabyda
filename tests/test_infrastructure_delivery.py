from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_terraform_is_provider_neutral_and_fail_closed() -> None:
    versions = _read("infra/terraform/versions.tf")
    variables = _read("infra/terraform/variables.tf")
    main = _read("infra/terraform/main.tf")
    cloud = _read("infra/terraform/cloud-init.yaml.tftpl")

    assert 'required_version = ">= 1.8.0, < 2.0.0"' in versions
    assert 'resource "terraform_data" "host_policy"' in main
    assert "provider \"" not in main
    assert "remote-exec" not in main
    assert "local-exec" not in main
    assert "0.0.0.0/0" in variables and "::/0" in variables
    assert "Dedicated paths must never resolve to a broad system directory" in main
    assert "ssh_pwauth: false" in cloud
    assert "disable_root: true" in cloud
    assert "ufw, default, deny, incoming" in cloud
    assert "timezone: UTC" in cloud
    assert "chrony" in cloud
    assert "no-new-privileges" in cloud
    assert "--project-name ${project_name}" in cloud
    assert "ExecCondition=/usr/bin/test ! -e ${app_dir}/.restore-maintenance" in cloud
    assert "groups: [adm]" in cloud
    assert "groups: [adm, docker" not in cloud
    assert "/usr/bin/docker compose *" not in cloud
    assert "LIVE_AUTOTRADE" not in cloud


def test_vault_agent_is_read_only_ephemeral_and_missing_key_strict() -> None:
    agent = _read("infra/vault/agent.hcl")
    policy = _read("infra/vault/funding-v1-policy.hcl")
    runtime = _read("infra/vault/runtime.env.ctmpl")
    telegram = _read("infra/vault/telegram.ctmpl")

    assert 'type       = "approle"' in agent
    assert "remove_secret_id_file_after_reading = true" in agent
    assert "exit_on_retry_failure" in agent
    assert agent.count("error_on_missing_key = true") == 5
    assert "sink" not in agent
    assert set(line.strip() for line in policy.splitlines() if "capabilities" in line) == {
        'capabilities = ["read"]'
    }
    for venue in (
        "BYBIT",
        "GATE",
        "OKX",
        "BINANCE",
        "HYPERLIQUID",
        "MEXC",
        "KUCOIN",
        "HTX",
    ):
        assert f"{venue}_API" in runtime or f"{venue}_PRIVATE_KEY" in runtime
    assert "TELEGRAM_BOT_TOKEN" in telegram
    assert "TELEGRAM_CHAT_ID" in telegram
    assert "CHANGE_ME" not in runtime + telegram


def test_compose_uses_optional_secret_overlays_and_live_requires_them() -> None:
    compose = yaml.safe_load(_read("docker-compose.yml"))
    live = _read(".env.live.example")
    env_files = compose["services"]["app"]["env_file"]

    assert env_files == [
        {"path": "${APP_ENV_FILE:-.env}", "required": True},
        {
            "path": "${APP_RUNTIME_SECRETS_ENV_FILE:-./secrets/exchange/runtime.env}",
            "required": False,
        },
        {
            "path": "${APP_TELEGRAM_SECRETS_ENV_FILE:-./secrets/exchange/telegram.env}",
            "required": False,
        },
    ]
    assert "APP_ENV_FILE=.env.live" in live
    assert "APP_RUNTIME_SECRETS_ENV_FILE=./secrets/exchange/runtime.env" in live
    assert "APP_TELEGRAM_SECRETS_ENV_FILE=./secrets/exchange/telegram.env" in live
    assert "./secrets/exchange/telegram-bot-token" in live
    assert "./secrets/exchange/telegram-chat-id" in live


def test_backup_is_stream_encrypted_atomic_and_scoped() -> None:
    backup = _read("scripts/backup_state.sh")

    assert "set -euo pipefail" in backup
    assert 'expected_project="funding_arbitrage_v1"' in backup
    assert ".funding-backup-root" in backup
    assert "pg_dump" in backup
    assert 'PGUSER="$POSTGRES_USER" PGDATABASE="$POSTGRES_DB"' in backup
    assert "PostgreSQL runtime credentials are missing" in backup
    assert "require_exact_line" in backup
    assert "postgres_exec pg_dump" in backup
    assert '| age --recipient "$recipient"' in backup
    assert "sha256sum" in backup
    assert "alembic_head" in backup
    assert "mktemp --tmpdir=\"$backup_root\"" in backup
    assert "mv -- \"$tmp_archive\" \"$archive\"" in backup
    assert "flock --nonblock 9" in backup
    assert "migration_head_before" in backup
    assert "migration_head_after" in backup
    assert 'mv -- "$tmp_complete" "$complete"' in backup
    assert "postgres_user=" not in backup
    assert "postgres_db=" not in backup
    assert "docker system prune" not in backup
    assert "rm -rf" not in backup


def test_restore_requires_safety_backup_stopped_app_and_transaction() -> None:
    restore = _read("scripts/restore_state.sh")

    assert "RESTORE_FUNDING_V1_POSTGRES_AND_KEEP_APP_STOPPED" in restore
    assert "RESTORE_CHANGE_TICKET" in restore
    assert "PRE_RESTORE_BACKUP" in restore
    assert 'if grep -Fxq app <<<"$running_services"' in restore
    assert "sha256sum --check --status" in restore
    assert "jq --exit-status" in restore
    assert ".compose_project == $project" in restore
    assert "PRE_RESTORE_BACKUP manifest must be newer" in restore
    assert "MAX_PRE_RESTORE_BACKUP_AGE_SECONDS" in restore
    assert "not a fresh current-state safety backup" in restore
    assert "does not match the current database migration head" in restore
    assert 'identity_file="${AGE_IDENTITY_FILE:-}"' in restore
    assert "AGE_IDENTITY_FILE must name one explicit private age identity file" in restore
    assert "without group/world access" in restore
    assert 'age --decrypt --identity "$identity_file"' in restore
    assert 'PGUSER="$POSTGRES_USER" PGDATABASE="$POSTGRES_DB"' in restore
    assert "PostgreSQL runtime credentials are missing" in restore
    assert "postgres_exec pg_restore" in restore
    assert "RESTORE_MAINTENANCE_MARKER" in restore
    assert "compose_root" in restore
    assert "fence beside the Compose file" in restore
    assert "require_exact_line" in restore
    assert 'expected_marker="funding-arbitrage-v1-restore:${change_ticket}"' in restore
    assert "identity_parent" in restore
    assert '"$candidate.complete"' in restore
    assert 'docker update --restart=no "$app_container_id"' in restore
    assert "RestartPolicy.Name" in restore
    assert "running_app_ids" in restore
    assert "State.Running" in restore
    assert "postgres_user=" not in restore
    assert "postgres_db=" not in restore
    assert "--clean --if-exists --single-transaction --exit-on-error" in restore
    assert "application remains stopped and fenced" in restore
    assert "docker system prune" not in restore
    assert "rm -rf" not in restore


def test_host_preflight_enforces_time_resources_ports_and_secret_modes() -> None:
    preflight = _read("scripts/host_preflight.sh")

    assert '"$(uname -s)" != "Linux"' in preflight
    assert "NTPSynchronized" in preflight
    assert "chronyc waitsync 10 0.1" in preflight
    assert "memory_kib < 3000000" in preflight
    assert "root_free_kib < 10485760" in preflight
    assert "for port in 5432 9108 9109" in preflight
    assert "forbidden public listener" in preflight
    assert "mode_value=$((8#$mode))" in preflight
    assert "(mode_value & 077) != 0" in preflight
    assert "10#$mode > 600" not in preflight
    assert "internal secret directory must be root-owned mode 0711" in preflight
    assert "private artifact owner is invalid" in preflight
    assert "check_private_owner secrets/internal/clickhouse-server.key 101 101" in preflight
    assert "secrets/internal/clickhouse-client.crt" in preflight
    assert "check_private_owner secrets/internal/clickhouse-client.key 101 101" in preflight


def test_ephemeral_pki_never_deletes_or_overwrites_a_destination() -> None:
    script = _read("scripts/generate_ephemeral_test_pki.sh")

    assert "mktemp -d" in script
    assert "refusing to overwrite existing PKI path" in script
    assert "rm -rf" not in script
    assert "EPHEMERAL_PKI_FORCE" not in script
    assert "printf '%s'" in script
    assert 'rm -f -- "$destination/ca.key" "$destination/ca.srl"' in script
    assert 'chmod 0711 "$destination"' in script
    assert 'chown 10001:10001 "$destination/app-client.key"' in script
    assert 'chown 70:70 "$destination/postgres-server.key"' in script
    assert 'chown 999:1000 "$destination/redis-server.key"' in script
    assert (
        'issue_certificate clickhouse-client funding clientAuth "DNS:clickhouse-client"'
        in script
    )
    assert (
        'chown 101:101 "$destination/clickhouse-server.key" "$destination/clickhouse-client.key"'
        in script
    )


def test_infrastructure_and_restore_runbooks_preserve_safety_boundary() -> None:
    infrastructure = " ".join(_read("ops/INFRASTRUCTURE_RUNBOOK.md").split())
    restore = " ".join(_read("ops/BACKUP_RESTORE_RUNBOOK.md").split())

    assert "new Ubuntu 24.04 VM only" in infrastructure
    assert "does not authorize" in infrastructure
    assert "LIVE_AUTOTRADE=false" in infrastructure
    assert "docker system prune" in infrastructure
    assert "projects other than `funding_arbitrage_v1`" in infrastructure
    assert "plaintext is never written to disk" in restore
    assert "disposable isolated VM" in restore
    assert "application remains stopped and fenced" in restore
    assert restore.index("RESTORE_MAINTENANCE_MARKER") < restore.index("systemctl stop")
    assert "--file docker-compose.yml up --detach postgres" in restore
    assert 'sudo rm -- "$RESTORE_MAINTENANCE_MARKER"' in restore
    assert "systemctl start funding-arbitrage-v1.service" in restore
    assert "`AGE_IDENTITY_FILE` is mandatory" in restore
    assert "quarterly" in restore.lower()
