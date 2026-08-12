from scripts.paper_acceptance_audit import build_audit


def _audit(execution_mode: str = "paper") -> dict[str, object]:
    return build_audit(
        {
            "status": "ok",
            "run_mode": "paper_test",
            "market_data_mode": "live_public",
            "execution_mode": execution_mode,
        },
        {"status": "ready", "comparison_enabled": True},
        {
            "canary": {"ready": True, "checks": {}},
            "checks": {"minimum_30_days": True},
            "evidence_ready": True,
            "accepted": True,
            "evidence_days": "30",
            "observation": {},
        },
        {"simulation_version": "candidate", "snapshot_count": 10},
        {"simulation_version": "baseline", "snapshot_count": 10},
        {"raw_candidates": 5},
        "candidate",
        "baseline",
    )


def test_acceptance_audit_requires_safe_runtime_and_all_gates() -> None:
    result = _audit()

    assert result["runtime_safe"] is True
    assert result["canary_ready"] is True
    assert result["acceptance_ready"] is True


def test_acceptance_audit_rejects_non_paper_execution() -> None:
    result = _audit("live")

    assert result["runtime_safe"] is False
    assert result["canary_ready"] is False
    assert result["acceptance_ready"] is False
