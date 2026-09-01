import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release-gate.yml"
SHADOW_SCRIPT_PATH = ROOT / "scripts" / "ci_shadow_smoke.sh"


def _workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def _workflow_jobs() -> dict[str, object]:
    payload = yaml.safe_load(_workflow_text())
    assert isinstance(payload, dict)
    jobs = payload.get("jobs")
    assert isinstance(jobs, dict)
    return jobs


def test_release_workflow_has_every_required_delivery_gate() -> None:
    jobs = _workflow_jobs()

    assert {
        "verify",
        "integration-replay",
        "load-slo",
        "attest-load-slo",
        "infrastructure-verify",
        "container-security",
        "restore-drill",
        "shadow-deploy",
        "publish-signed-image",
        "manual-live-gate",
    }.issubset(jobs)
    assert "coverage_gate.py" in str(jobs["verify"])
    assert "test_historical_replay.py" in str(jobs["integration-replay"])
    assert "scripts/load_slo.py" in str(jobs["load-slo"])
    assert "--events 20000" in str(jobs["load-slo"])
    assert "--decisions 5000" in str(jobs["load-slo"])
    assert "--release-evidence" in str(jobs["load-slo"])
    assert '--revision "$GITHUB_SHA"' in str(jobs["load-slo"])
    assert "--evidence-source github-actions" in str(jobs["load-slo"])
    assert '--github-run-id "$GITHUB_RUN_ID"' in str(jobs["load-slo"])
    assert '--github-run-attempt "$GITHUB_RUN_ATTEMPT"' in str(jobs["load-slo"])
    assert (
        "funding-load-slo-${{ github.sha }}-${{ github.run_id }}-"
        "${{ github.run_attempt }}"
    ) in str(jobs["load-slo"])
    assert "funding-load-slo.json.sha256" in str(jobs["load-slo"])
    assert "funding-load-slo-run.txt" in str(jobs["load-slo"])
    assert "funding-load-slo.log" in str(jobs["load-slo"])
    assert "set -o pipefail" in str(jobs["load-slo"])
    assert jobs["load-slo"]["permissions"] == {"contents": "read"}
    assert jobs["attest-load-slo"]["needs"] == "load-slo"
    assert jobs["attest-load-slo"]["if"] == (
        "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    )
    assert "actions/attest@a1948c3f048ba23858d222213b7c278aabede763" in str(
        jobs["attest-load-slo"]
    )
    assert jobs["attest-load-slo"]["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "attestations": "write",
        "artifact-metadata": "write",
    }
    assert set(jobs["publish-signed-image"]["needs"]) == {
        "shadow-deploy",
        "attest-load-slo",
    }
    assert "'retention-days': 30" in str(jobs["load-slo"])
    assert "terraform -chdir=infra/terraform validate" in str(jobs["infrastructure-verify"])
    assert "scripts/backup_state.sh" in str(jobs["infrastructure-verify"])
    assert "scripts/ci_restore_drill.sh" in str(jobs["restore-drill"])
    assert "restore-drill" in jobs["shadow-deploy"]["needs"]
    restore_drill = (ROOT / "scripts" / "ci_restore_drill.sh").read_text(encoding="utf-8")
    assert "BACKUP_FUNDING_V1_POSTGRES_WHILE_APP_STOPPED_AND_FENCED" in (
        (ROOT / "scripts" / "ci_restore_drill.sh").read_text(encoding="utf-8")
    )
    assert '${GITHUB_ACTIONS:-}' in restore_drill
    assert '${CI:-}' in restore_drill
    assert '${RUNNER_TEMP:-}' in restore_drill
    assert 'label=com.docker.compose.project=$project' in restore_drill
    assert "cleanup_authorized=false" in restore_drill
    assert 'if [[ "$cleanup_authorized" == "true" ]]' in restore_drill
    assert 'sudo find "$work_root" -depth -delete' in restore_drill
    assert "restore drill refuses an existing Compose project" in restore_drill
    assert "restore drill refuses to overwrite an existing repository artifact" in restore_drill
    assert "rm -rf" not in restore_drill
    workflow = _workflow_text()
    assert "for script in \\" in workflow
    assert workflow.count('bash -n "$script"') == 1
    assert '"$artifact_dir/.release-sha"' in workflow
    assert "include-hidden-files: true" in workflow
    assert "pip_audit" in str(jobs["container-security"])
    assert "trivy-action" in str(jobs["container-security"])
    assert "sbom-action" in str(jobs["container-security"])
    assert "cosign" in str(jobs["container-security"]).lower()


def test_all_action_dependencies_are_immutable_and_expected() -> None:
    workflow = _workflow_text()
    refs = re.findall(r"uses:\s+([^@\s]+)@([^\s]+)", workflow)

    assert refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", revision) for _, revision in refs)
    assert (
        "aquasecurity/trivy-action",
        "a9c7b0f06e461e9d4b4d1711f154ee024b8d7ab8",
    ) in refs
    assert (
        "anchore/sbom-action",
        "e22c389904149dbc22b58101806040fa8d37a610",
    ) in refs
    assert (
        "sigstore/cosign-installer",
        "6f9f17788090df1f26f669e9d70d6ae9567deba6",
    ) in refs


def test_shadow_deployment_is_isolated_and_cannot_trade() -> None:
    script = SHADOW_SCRIPT_PATH.read_text(encoding="utf-8")

    assert "docker network create --internal" in script
    assert "TRADING_MODE=SHADOW" in script
    assert "MARKET_DATA_MODE=mock" in script
    assert "EXECUTION_MODE=paper" in script
    assert "PAPER_AUTOTRADE=false" in script
    assert "LIVE_AUTOTRADE=false" in script
    assert "--cap-drop ALL" in script
    assert "no-new-privileges:true" in script
    assert "--user 70:70" in script
    assert "--user 10001:10001" in script
    assert script.count("--init") == 2
    assert "--pids-limit 128" in script
    assert "--pids-limit 256" in script
    assert "--memory 384m" in script
    assert "--memory 1024m" in script
    assert script.count("--cpus") == 2
    assert "docker push" not in script
    assert "LIVE_ARMED=true" not in script
    assert not re.search(r"--publish|\s-p\s", script)
    assert not re.search(r"(?:API_KEY|API_SECRET)=", script)


def test_publish_and_manual_gate_are_tightly_scoped() -> None:
    jobs = _workflow_jobs()
    publish = jobs["publish-signed-image"]
    manual = jobs["manual-live-gate"]
    workflow = _workflow_text()

    assert isinstance(publish, dict)
    assert publish["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    assert publish["permissions"] == {
        "contents": "read",
        "packages": "write",
        "id-token": "write",
    }
    assert isinstance(manual, dict)
    assert manual["if"] == "github.event_name == 'workflow_dispatch'"
    assert manual["environment"] == "limited-live-approval"
    assert manual["permissions"] == {"contents": "read"}
    assert "manual_live_gate.py" in str(manual)
    assert "secrets." not in workflow
    assert not re.search(r"\b(?:ssh|scp|rsync)\b", workflow, re.IGNORECASE)


def test_paper_test_template_satisfies_required_compose_variables() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    environment = dict(
        line.split("=", 1)
        for line in (ROOT / ".env.paper-test.example")
        .read_text(encoding="utf-8")
        .splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    required = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*):\?", compose))

    assert required
    assert required <= environment.keys()
    assert all(environment[name] for name in required)
