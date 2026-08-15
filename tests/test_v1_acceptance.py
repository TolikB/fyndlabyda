from __future__ import annotations

from pathlib import Path

from funding_arbitrage.acceptance import acceptance_summary, load_manifest, main, validate_manifest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config" / "v1_acceptance.yaml"


def test_full_v1_manifest_is_structurally_valid_and_has_no_deferred_release() -> None:
    manifest = load_manifest(MANIFEST)

    assert validate_manifest(manifest, repository_root=ROOT) == []
    assert manifest["release"]["all_scope_single_v1"] is True
    assert manifest["release"]["includes_original_exclusions"] is True
    assert manifest["release"]["includes_original_v2"] is True
    assert manifest["release"]["deferred_releases"] == []


def test_manifest_records_dangerous_features_but_keeps_them_fail_closed() -> None:
    manifest = load_manifest(MANIFEST)
    safety = manifest["safety"]

    assert safety["explicit_live_authorization_required"] is True
    assert safety["real_orders_forbidden_by_default"] is True
    assert safety["withdrawals_forbidden_by_default"] is True
    assert safety["dangerous_capabilities_default_enabled"] is False
    assert {
        "automated_withdrawals",
        "dex_execution",
        "grid_averaging",
        "live_llm_decisions",
        "live_rl_decisions",
        "loss_averaging",
        "martingale",
        "mev_execution",
    }.issubset(safety["dangerous_capabilities"])


def test_acceptance_gate_cannot_pass_while_requirements_are_incomplete() -> None:
    manifest = load_manifest(MANIFEST)
    summary = acceptance_summary(manifest)

    assert summary["complete"] is False
    assert summary["requirements"] >= 50
    assert main(["--manifest", str(MANIFEST), "--require-accepted"]) == 3
