"""A new interpreter must start each audit without imports preloaded by pytest."""
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("name", [
    "audit_risk_metric_formula_shadow", "audit_low_vol_risk_metric_distortion",
    "audit_core_dynamic_overlap", "audit_tracker_auxiliary_scores",
])
@pytest.mark.parametrize("as_module", [False, True])
def test_audit_cli_starts_in_a_fresh_interpreter(name, as_module):
    arguments = ["-m", "scripts." + name] if as_module else ["scripts/" + name + ".py"]
    result = subprocess.run([sys.executable, "-B", *arguments, "--help"], cwd=ROOT,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "usage:" in result.stdout.lower()
    assert "--help" in result.stdout
