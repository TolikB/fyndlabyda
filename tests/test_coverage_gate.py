from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.coverage_gate import CRITICAL_FILES, evaluate_coverage, main


def _payload(
    *,
    overall_covered: int = 85,
    critical_covered: int = 95,
) -> dict[str, object]:
    return {
        "totals": {
            "covered_lines": overall_covered,
            "num_statements": 100,
        },
        "files": {
            name.replace("/", "\\"): {
                "summary": {
                    "covered_lines": critical_covered,
                    "num_statements": 100,
                }
            }
            for name in CRITICAL_FILES
        },
    }


def test_coverage_gate_accepts_exact_thresholds_on_windows_paths() -> None:
    result = evaluate_coverage(_payload())

    assert result["passed"] is True
    assert result["overall_percent"] == 85.0
    assert result["critical_percent"] == 95.0


@pytest.mark.parametrize(
    ("overall", "critical"),
    [(84, 95), (85, 94), (84, 94)],
)
def test_coverage_gate_rejects_either_threshold_below_contract(
    overall: int,
    critical: int,
) -> None:
    result = evaluate_coverage(
        _payload(overall_covered=overall, critical_covered=critical)
    )

    assert result["passed"] is False


def test_coverage_gate_rejects_missing_or_invalid_critical_data() -> None:
    missing = _payload()
    del missing["files"][CRITICAL_FILES[0].replace("/", "\\")]  # type: ignore[index]
    with pytest.raises(ValueError, match="critical coverage files missing"):
        evaluate_coverage(missing)

    invalid = _payload()
    invalid["totals"] = {"covered_lines": 101, "num_statements": 100}
    with pytest.raises(ValueError, match="counts are invalid"):
        evaluate_coverage(invalid)


def test_coverage_gate_cli_returns_distinct_failure_codes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = tmp_path / "coverage.json"
    report.write_text(json.dumps(_payload()), encoding="utf-8")
    assert main(["--report", str(report)]) == 0
    assert "overall=85.00%/85.00%" in capsys.readouterr().out

    report.write_text(json.dumps(_payload(overall_covered=84)), encoding="utf-8")
    assert main(["--report", str(report)]) == 1
    assert "overall=84.00%/85.00%" in capsys.readouterr().out

    report.write_text("not-json", encoding="utf-8")
    assert main(["--report", str(report)]) == 2
    assert json.loads(capsys.readouterr().out)["passed"] is False

