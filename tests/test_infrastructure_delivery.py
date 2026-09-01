import os
import shutil
import subprocess
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
    assert 'regex("^[0-9.]+/32$"' in variables
    assert 'regex("^[0-9A-Fa-f:]+/128$"' in variables
    assert 'trimprefix(var.app_dir, "/opt/")' in variables
    assert 'trimprefix(var.data_dir, "/srv/")' in variables
    assert 'trimprefix(var.backup_root, "/var/backups/")' in variables
    assert '!contains([".", ".."], segment)' in variables
    assert "templatefile(" in main
    assert "app_dir = var.app_dir" in main
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
    assert "  - funding\n\nusers:" in cloud
    assert "uid: 10001" in cloud
    assert "primary_group: funding" in cloud
    assert cloud.count("    defer: true") == 7
    assert "LoadCredentialEncrypted=approle-secret-id:" in cloud
    assert "ExecStartPre=/usr/local/sbin/funding-v1-vault-render-gate clear" in cloud
    assert "ExecStartPost=/usr/local/sbin/funding-v1-vault-render-gate wait" in cloud
    assert "Vault did not freshly render every required secret within 60 seconds" in cloud
    assert 'for name in "$${required_files[@]}"' in cloud
    assert "BindsTo=vault-agent-funding.service" in cloud
    assert "ExecStartPre=/usr/bin/bash scripts/host_preflight.sh" in cloud
    assert "ConditionPathExists=${app_dir}/docker-compose.production.yml" in cloud
    assert "ConditionPathExists=${app_dir}/.env.release" in cloud
    assert "--file docker-compose.yml --file docker-compose.production.yml" in cloud
    assert "--env-file .env.live --env-file .env.release" in cloud
    assert "--no-build" in cloud
    assert "--pull never" not in cloud
    assert "/etc/credstore.encrypted/funding-v1-approle-secret-id" in cloud
    assert "/usr/local/sbin/funding-v1-control start" in cloud
    assert "#!/usr/bin/bash" in cloud
    assert 'if [[ "$EUID" -ne 0 || "$#" -ne 1 ]]' in cloud
    assert "unsupported funding-v1-control action" in cloud
    assert 'readonly app_dir="${app_dir}"' in cloud
    assert '[install, -d, -m, "0750", -o, root, -g, funding, ${app_dir}]' in cloud
    assert "LIVE_AUTOTRADE" not in cloud


def test_vault_agent_is_read_only_ephemeral_and_missing_key_strict() -> None:
    agent = _read("infra/vault/agent.hcl")
    policy = _read("infra/vault/funding-v1-policy.hcl")
    runtime = _read("infra/vault/runtime.env.ctmpl")
    telegram = _read("infra/vault/telegram.ctmpl")

    assert 'type       = "approle"' in agent
    assert "remove_secret_id_file_after_reading = false" in agent
    assert (
        'secret_id_file_path                 = '
        '"/run/credentials/vault-agent-funding.service/approle-secret-id"'
    ) in agent
    assert "exit_on_retry_failure" in agent
    assert agent.count("error_on_missing_key = true") == 5
    assert "sink" not in agent
    assert agent.count('destination          = "${app_dir}/secrets/exchange/') == 5
    assert "/opt/funding-arbitrage-v1/secrets/exchange" not in agent
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
    alertmanager = compose["services"]["alertmanager"]
    assert alertmanager["user"] == "10001:10001"
    assert "uid=10001,gid=10001" in alertmanager["tmpfs"][0]
    assert compose["services"]["app"]["restart"] == "no"
    assert compose["services"]["app"]["image"] == (
        "${APP_IMAGE:-funding-arbitrage:local}"
    )


def test_production_compose_requires_verified_image_without_pull_or_build() -> None:
    compose = yaml.safe_load(_read("docker-compose.production.yml"))

    for service_name in ("app", "low-latency"):
        service = compose["services"][service_name]
        assert service["image"] == (
            "${APP_IMAGE:?APP_IMAGE must be an immutable verified digest}"
        )
        assert service["pull_policy"] == "never"


def test_acceptance_compose_overlay_pins_measured_image_and_evidence_paths() -> None:
    compose = yaml.safe_load(_read("docker-compose.acceptance.yml"))
    app = compose["services"]["app"]

    assert app["image"] == (
        "${ACCEPTANCE_IMAGE:?ACCEPTANCE_IMAGE must be the measured sha256 image ID}"
    )
    assert app["pull_policy"] == "never"
    assert app["volumes"] == [
        (
            "${ACCEPTANCE_EVIDENCE_DIR:?ACCEPTANCE_EVIDENCE_DIR must be set}:"
            "/var/lib/funding-arbitrage/acceptance"
        ),
        (
            "${ACCEPTANCE_RELEASE_IDENTITY_FILE:?ACCEPTANCE_RELEASE_IDENTITY_FILE "
            "must be set}:/run/funding-arbitrage/release-identity.json:ro"
        ),
    ]
    assert "privileged" not in app


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
    assert "RELEASE_COMMIT_SHA" in backup
    assert '"$script_root/.release-sha"' in backup
    assert "release commit provenance sources disagree" in backup
    assert 'commit_sha="unknown"' not in backup
    assert backup.count("resolve_release_commit") == 3
    assert "--untracked-files=no" in backup
    assert "org.opencontainers.image.revision" in backup
    assert "application image revision does not match" in backup
    assert "release provenance changed while backup was running" in backup
    assert 'allow_stopped_app="${BACKUP_ALLOW_STOPPED_APP:-false}"' in backup
    assert "BACKUP_FUNDING_V1_POSTGRES_WHILE_APP_STOPPED_AND_FENCED" in backup
    assert "stopped-app backup requires a stopped restart-fenced application container" in backup
    assert "verify_stopped_app_backup_fence" in backup
    assert 'verified_maintenance_marker=""' in backup
    assert 'exec 8<"$verified_maintenance_marker"' in backup
    assert "another funding restore or stopped-app backup is already running" in backup
    assert backup.index('exec 8<"$verified_maintenance_marker"') < backup.index(
        'commit_sha_before="$(resolve_release_commit)"'
    )
    assert 'RestartPolicy.Name' in backup
    assert "manifest_hash" in backup
    assert '"$(basename "$manifest")" > "$tmp_complete"' in backup
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
    assert ".git_commit" in restore
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
    assert 'postgres_restore_archive "$restore_database"' in restore
    assert "postgres_archive_validate" in restore
    assert "pg_restore --list" in restore
    assert "workspace=/dev/shm/funding-arbitrage-v1-restore" in restore
    assert 'archive_path="$workspace/apply.dump"' in restore
    assert 'archive_path="$workspace/list.dump"' in restore
    assert "postgres_archive_workspace_cleanup" in restore
    assert "insufficient PostgreSQL shared-memory capacity" in restore
    assert "RESTORE_MAINTENANCE_MARKER" in restore
    assert "compose_root" in restore
    assert "fence beside the Compose file" in restore
    assert "require_exact_line" in restore
    assert 'expected_marker="funding-arbitrage-v1-restore:$change_ticket"' in restore
    assert "identity_parent" in restore
    assert '"$candidate.complete"' in restore
    assert "flock --nonblock 8" in restore
    assert "another funding restore or stopped-app backup is already running" in restore
    assert "expected_completion" in restore
    assert "actual_manifest_hash" in restore
    assert 'sha256sum --check --status "$(basename "$candidate.complete")"' in restore
    assert 'docker update --restart=no "$app_container_id"' in restore
    assert "RestartPolicy.Name" in restore
    assert "running_app_ids" in restore
    assert "State.Running" in restore
    assert "postgres_user=" not in restore
    assert "postgres_db=" not in restore
    assert "unexpected_current_schemas" in restore
    assert 'validate_restored_database "$restore_database"' in restore
    assert 'validate_restored_database "$database_name"' in restore
    assert "did not reach the expected Alembic migration head" in restore
    assert "critical-table count is invalid" in restore
    assert "is missing required application tables" in restore
    assert "relation.relkind IN ('r', 'p')" in restore
    assert "oms_order_states" in restore
    assert "execution_fills" in restore
    assert "position_states" in restore
    assert "ledger_transactions" in restore
    assert "ledger_postings" in restore
    assert "immutable_audit_log" in restore
    assert "createdb --maintenance-db=postgres --template=template0" in restore
    assert "ALLOW_CONNECTIONS $extra_name" in restore
    assert 'postgres_admin allow "$database_name" false' in restore
    assert "pg_terminate_backend" in restore
    assert "current_setting(\\$\\$funding.restore_database\\$\\$)" in restore
    assert "pg_terminate_backend(pid, 5000)" in restore
    assert "remaining_sessions" in restore
    assert 'postgres_admin rename "$database_name" "$rollback_database"' in restore
    assert "write_swap_stage original_renamed" in restore
    assert "write_swap_stage replacement_renamed" in restore
    assert "ticket_hash: $ticket_hash" in restore
    assert "archive_sha256: $archive_sha256" in restore
    assert "safety_sha256: $safety_sha256" in restore
    assert "type == \"object\" and length == 8" in restore
    assert "does not match this exact restore operation" in restore
    assert "reconcile_interrupted_swap" in restore
    assert "database_presence" in restore
    assert "could not query PostgreSQL database identity" in restore
    assert 'remove_swap_stage || return 1' in restore
    assert restore.index("reconcile_interrupted_swap") < restore.index("pre_restore_iso=")
    assert 'postgres_admin probe "$database_name"' in restore
    assert "automatic database-swap recovery is incomplete" in restore
    assert "RESTORE_TMPFS_DIR" in restore
    assert "findmnt --noheadings --output FSTYPE" in restore
    assert 'target_plain="$restore_tmpfs_dir/target.dump"' in restore
    assert 'safety_plain="$restore_tmpfs_dir/safety.dump"' in restore
    assert "insufficient host tmpfs capacity" in restore
    assert restore.index('target_plain="$restore_tmpfs_dir/target.dump"') < restore.index(
        "pre_restore_iso="
    )
    assert restore.index("flock --nonblock 8") < restore.index(
        'target_plain="$restore_tmpfs_dir/target.dump"'
    )
    assert restore.index("postgres_archive_workspace_cleanup") < restore.index("pre_restore_iso=")
    assert "POSTGRES_DB cannot be a PostgreSQL maintenance" in restore
    assert restore.index('age --decrypt --identity "$identity_file" "$archive"') < restore.index(
        'postgres_admin create "$restore_database"'
    )
    assert "--single-transaction --exit-on-error --no-owner --no-acl" in restore
    assert "DROP SCHEMA" not in restore
    assert "application remains stopped and fenced" in restore
    assert "docker system prune" not in restore
    assert "rm -rf" not in restore


def test_restore_drill_emits_private_typed_release_bound_evidence() -> None:
    drill = _read("scripts/ci_restore_drill.sh")
    workflow = yaml.safe_load(_read(".github/workflows/release-gate.yml"))
    restore_job = workflow["jobs"]["restore-drill"]

    assert "funding-disaster-recovery.json" in drill
    assert "disaster_recovery_evidence.py" in drill
    assert "source_event_count_before_restore" in drill
    assert "restored_post_target_event_count" in drill
    assert "wrong_ticket_rejected: true" in drill
    assert "target_catalog_verified: true" in drill
    assert "safety_catalog_verified: true" in drill
    assert "app_running_during_restore: false" in drill
    assert "database_plaintext_artifact_count: 0" in drill
    assert "scripts/disaster_recovery_evidence.py verify" in str(restore_job)
    assert "actions/upload-artifact" not in str(restore_job)


def test_host_preflight_enforces_time_resources_ports_and_secret_modes() -> None:
    preflight = _read("scripts/host_preflight.sh")

    assert '"$(uname -s)" != "Linux"' in preflight
    assert "NTPSynchronized" in preflight
    assert "chronyc waitsync 10 0.1" in preflight
    assert "memory_kib < 6291456" in preflight
    assert "root_free_kib < 10485760" in preflight
    assert "for port in 5432 9108 9109" in preflight
    assert "forbidden public listener" in preflight
    assert "mode_value=$((8#$mode))" in preflight
    assert "(mode_value & 077) != 0" in preflight
    assert "10#$mode > 600" not in preflight
    assert "internal secret directory must be root-owned mode 0711" in preflight
    assert "private artifact owner is invalid" in preflight
    assert (
        "for command_name in chronyc docker findmnt git id jq setpriv "
        "sha256sum ss stat timedatectl"
    ) in preflight
    assert "POSTGRES_USER" in preflight
    assert "check_private_owner secrets/internal/clickhouse-server.key 101 101" in preflight
    assert "secrets/internal/clickhouse-client.crt" in preflight
    verifier = _read("scripts/verify_internal_tls.sh")
    assert "openssl pkey -check -noout -passin pass:" in verifier
    assert "check_postgres_client_cn" in verifier
    assert "internal TLS directory must not contain a CA private key" in verifier
    assert "check_private_owner secrets/internal/clickhouse-client.key 101 101" in preflight
    assert 'funding_uid="$(id -u funding)"' in preflight
    assert 'funding_gid="$(id -g funding)"' in preflight
    assert 'exchange_dir_uid="$(stat -c \'%u\' secrets/exchange)"' in preflight
    assert "exchange secret directory must be UID 10001 mode 0700" in preflight
    assert "check_private_uid \"$rendered_file\" 10001" in preflight
    assert 'setpriv --reuid=10001 --regid="$funding_gid" --clear-groups' in preflight
    assert "runtime UID 10001 cannot read rendered secret" in preflight
    assert "secrets/exchange/telegram-bot-token" in preflight
    assert "secrets/exchange/telegram-chat-id" in preflight
    assert 'readonly release_env_file=".env.release"' in preflight
    assert 'readonly production_compose_file="docker-compose.production.yml"' in preflight
    assert "ghcr.io/tolikb/fyndlabyda" in preflight
    assert "release metadata must be a root:funding mode-0640 regular file" in preflight
    assert "deployed checkout does not match the immutable release commit" in preflight
    assert "for service_name in app low-latency" in preflight
    assert "production Compose service $service_name does not select" in preflight
    assert "local application image does not match release commit" in preflight
    assert 'index($image) != null' in preflight
    assert (
        'bash scripts/verify_internal_tls.sh secrets/internal 86400 "$postgres_user"'
        in preflight
    )


def test_signed_release_staging_verifies_before_pull_and_never_starts_app() -> None:
    staging = _read("scripts/stage_signed_release.sh")

    assert 'readonly expected_repository="ghcr.io/tolikb/fyndlabyda"' in staging
    assert "mktemp mv realpath sha256sum" in staging
    assert "cosign verify" in staging
    assert "--certificate-identity" in staging
    assert "--certificate-oidc-issuer" in staging
    assert staging.index("cosign verify") < staging.index('docker pull "$image_ref"')
    assert "org.opencontainers.image.revision" in staging
    assert "status --porcelain --untracked-files=no" in staging
    assert "APP_IMAGE=%s" in staging
    assert "RELEASE_COMMIT_SHA=%s" in staging
    assert staging.count("mv -fT --") == 4
    assert "application remains stopped" in staging
    assert "docker compose up" not in staging
    assert "systemctl start" not in staging
    assert "rm -rf" not in staging


def test_internal_tls_verifier_rejects_invalid_runtime_material(
    tmp_path: Path,
) -> None:
    if (
        os.name != "posix"
        or shutil.which("bash") is None
        or shutil.which("openssl") is None
    ):
        return

    generator = ROOT / "scripts" / "generate_ephemeral_test_pki.sh"
    verifier = ROOT / "scripts" / "verify_internal_tls.sh"
    signing = tmp_path / "signing"
    valid = tmp_path / "valid"
    fixtures = tmp_path / "issued"
    second = tmp_path / "second"
    environment = {
        **os.environ,
        "ALLOW_EPHEMERAL_TEST_PKI": "YES",
        "KEEP_EPHEMERAL_TEST_CA_KEY": "YES",
    }
    subprocess.run(
        ["bash", str(generator), str(signing)],
        check=True,
        env=environment,
        timeout=30,
    )
    shutil.copytree(signing, valid, ignore=shutil.ignore_patterns("ca.key"))
    fixtures.mkdir()
    second_environment = {**os.environ, "ALLOW_EPHEMERAL_TEST_PKI": "YES"}
    subprocess.run(
        ["bash", str(generator), str(second)],
        check=True,
        env=second_environment,
        timeout=30,
    )

    def verify(
        path: Path,
        minimum_validity: str = "86400",
        postgres_username: str = "funding",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "bash",
                str(verifier),
                str(path),
                minimum_validity,
                postgres_username,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def issue_fixture(
        name: str,
        common_name: str,
        usage: str,
        san: str,
        organization: str | None = None,
        multi_valued_rdn: bool = False,
        additional_common_name: str | None = None,
    ) -> tuple[Path, Path]:
        key = fixtures / f"{name}.key"
        request = fixtures / f"{name}.csr"
        certificate = fixtures / f"{name}.crt"
        subject = f"/CN={common_name}"
        if organization is not None:
            subject += f"{'+' if multi_valued_rdn else '/'}O={organization}"
        if additional_common_name is not None:
            subject += f"/CN={additional_common_name}"
        subprocess.run(
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-nodes",
                "-subj",
                subject,
                "-addext",
                f"subjectAltName={san}",
                "-addext",
                f"extendedKeyUsage={usage}",
                "-keyout",
                str(key),
                "-out",
                str(request),
            ],
            check=True,
            capture_output=True,
            timeout=20,
        )
        subprocess.run(
            [
                "openssl",
                "x509",
                "-req",
                "-sha256",
                "-days",
                "2",
                "-in",
                str(request),
                "-CA",
                str(signing / "ca.crt"),
                "-CAkey",
                str(signing / "ca.key"),
                "-CAcreateserial",
                "-copy_extensions",
                "copy",
                "-out",
                str(certificate),
            ],
            check=True,
            capture_output=True,
            timeout=20,
        )
        request.unlink()
        return certificate, key

    assert verify(valid).returncode == 0
    assert verify(valid, "999999999").returncode != 0
    assert verify(valid, postgres_username="unsafe user").returncode == 2

    leaked_ca = tmp_path / "leaked-ca"
    shutil.copytree(valid, leaked_ca)
    shutil.copy2(signing / "ca.key", leaked_ca / "ca.key")
    assert verify(leaked_ca).returncode != 0

    wrong_ca = tmp_path / "wrong-ca"
    shutil.copytree(valid, wrong_ca)
    shutil.copy2(second / "ca.crt", wrong_ca / "ca.crt")
    assert verify(wrong_ca).returncode != 0

    mismatched = tmp_path / "mismatched"
    shutil.copytree(valid, mismatched)
    shutil.copy2(valid / "postgres-server.key", mismatched / "app-client.key")
    assert verify(mismatched).returncode != 0

    encrypted = tmp_path / "encrypted"
    shutil.copytree(valid, encrypted)
    subprocess.run(
        [
            "openssl",
            "pkey",
            "-in",
            str(valid / "app-client.key"),
            "-aes-256-cbc",
            "-passout",
            "pass:test-only",
            "-out",
            str(encrypted / "app-client.key"),
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
    assert verify(encrypted).returncode != 0

    wrong_host_cert, wrong_host_key = issue_fixture(
        "redis-wrong-host",
        "redis",
        "serverAuth",
        "DNS:not-redis",
    )
    wrong_hostname = tmp_path / "wrong-hostname"
    shutil.copytree(valid, wrong_hostname)
    shutil.copy2(wrong_host_cert, wrong_hostname / "redis-server.crt")
    shutil.copy2(wrong_host_key, wrong_hostname / "redis-server.key")
    assert verify(wrong_hostname).returncode != 0

    wrong_purpose_cert, wrong_purpose_key = issue_fixture(
        "redis-wrong-purpose",
        "redis",
        "clientAuth",
        "DNS:redis",
    )
    wrong_purpose = tmp_path / "wrong-purpose"
    shutil.copytree(valid, wrong_purpose)
    shutil.copy2(wrong_purpose_cert, wrong_purpose / "redis-server.crt")
    shutil.copy2(wrong_purpose_key, wrong_purpose / "redis-server.key")
    assert verify(wrong_purpose).returncode != 0

    wrong_cn_cert, wrong_cn_key = issue_fixture(
        "app-wrong-cn",
        "other_user",
        "clientAuth",
        "DNS:funding",
    )
    wrong_cn = tmp_path / "wrong-cn"
    shutil.copytree(valid, wrong_cn)
    shutil.copy2(wrong_cn_cert, wrong_cn / "app-client.crt")
    shutil.copy2(wrong_cn_key, wrong_cn / "app-client.key")
    assert verify(wrong_cn).returncode != 0
    assert verify(wrong_cn, postgres_username="other_user").returncode == 0

    extra_subject_cert, extra_subject_key = issue_fixture(
        "app-extra-subject",
        "funding",
        "clientAuth",
        "DNS:funding",
        organization="Example",
    )
    extra_subject = tmp_path / "extra-subject"
    shutil.copytree(valid, extra_subject)
    shutil.copy2(extra_subject_cert, extra_subject / "app-client.crt")
    shutil.copy2(extra_subject_key, extra_subject / "app-client.key")
    assert verify(extra_subject).returncode == 0

    multi_rdn_cert, multi_rdn_key = issue_fixture(
        "app-multi-rdn",
        "funding",
        "clientAuth",
        "DNS:funding",
        organization="Example",
        multi_valued_rdn=True,
    )
    multi_rdn = tmp_path / "multi-rdn"
    shutil.copytree(valid, multi_rdn)
    shutil.copy2(multi_rdn_cert, multi_rdn / "app-client.crt")
    shutil.copy2(multi_rdn_key, multi_rdn / "app-client.key")
    assert verify(multi_rdn).returncode == 0

    duplicate_cn_cert, duplicate_cn_key = issue_fixture(
        "app-duplicate-cn",
        "funding",
        "clientAuth",
        "DNS:funding",
        additional_common_name="funding",
    )
    duplicate_cn = tmp_path / "duplicate-cn"
    shutil.copytree(valid, duplicate_cn)
    shutil.copy2(duplicate_cn_cert, duplicate_cn / "app-client.crt")
    shutil.copy2(duplicate_cn_key, duplicate_cn / "app-client.key")
    assert verify(duplicate_cn).returncode != 0

    malformed = tmp_path / "malformed"
    shutil.copytree(valid, malformed)
    (malformed / "app-client.crt").write_text(
        "not a certificate\n",
        encoding="utf-8",
    )
    assert verify(malformed).returncode != 0


def test_ephemeral_pki_never_deletes_or_overwrites_a_destination() -> None:
    script = _read("scripts/generate_ephemeral_test_pki.sh")

    assert "mktemp -d" in script
    assert "refusing to overwrite existing PKI path" in script
    assert "rm -rf" not in script
    assert "EPHEMERAL_PKI_FORCE" not in script
    assert "printf '%s'" in script
    assert 'rm -f -- "$destination/ca.srl"' in script
    assert "KEEP_EPHEMERAL_TEST_CA_KEY" in script
    assert 'rm -f -- "$destination/ca.key"' in script
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
    assert "The source commit is mandatory" in restore
    assert "`unknown` provenance is rejected" in restore
    assert "disposable isolated VM" in restore
    assert "former one-line `.complete` marker" in restore
    assert "separately reviewed offline recovery" in restore
    assert "application remains stopped and fenced" in restore
    assert restore.index("RESTORE_MAINTENANCE_MARKER") < restore.index("systemctl stop")
    assert "--file docker-compose.yml up --detach postgres" in restore
    assert 'sudo rm -- "$RESTORE_MAINTENANCE_MARKER"' in restore
    assert "HostConfig.RestartPolicy.Name" in restore
    assert "= no" in restore
    assert "funding-v1-control start" in restore
    assert "`AGE_IDENTITY_FILE` is mandatory" in restore
    assert "quarterly" in restore.lower()
