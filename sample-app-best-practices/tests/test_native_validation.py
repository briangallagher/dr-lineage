from __future__ import annotations

import json
from pathlib import Path

from lineage_demo.config import Settings
from lineage_demo.native_validation import (
    build_native_path_validation,
    write_native_path_validation,
)
from lineage_demo.product_demo import build_product_demo

FIXTURE_ROOT = Path(__file__).parents[1]
LIVE_BASELINE = (
    FIXTURE_ROOT / "examples/native-kfp-validation/live-read-only-baseline-2026-09-30.json"
)


def test_native_path_validation_passes_the_four_local_checks(tmp_path) -> None:
    document = build_product_demo(Settings(), FIXTURE_ROOT)

    validation = build_native_path_validation(document)
    result = write_native_path_validation(validation, tmp_path / "validation")

    assert validation["status"] == "PASS"
    assert validation["recommendation"] == "GO_FOR_NARROW_NATIVE_SPIKE"
    assert [item["status"] for item in validation["checks"]] == ["PASS"] * 4
    assert validation["notProductionEvidence"] is True
    assert json.loads(Path(result["validation"]).read_text())["profile"] == (
        "rhoai-native-kfp-poc-validation"
    )


def test_live_baseline_records_partial_native_result_without_secrets() -> None:
    baseline = json.loads(LIVE_BASELINE.read_text(encoding="utf-8"))
    serialized = json.dumps(baseline).lower()

    assert baseline["evidenceLevel"] == "VERIFIED_READ_ONLY"
    assert baseline["kfp"]["state"] == "SUCCEEDED"
    assert baseline["openLineage"]["nativeKfpFacetObserved"] is False
    assert {item["result"] for item in baseline["checks"]} == {
        "PASS",
        "BLOCKED",
        "PARTIAL",
        "NOT_EXERCISED",
    }
    assert baseline["recommendation"] == {
        "currentNativeSupport": "NO_GO",
        "nextImplementationSpike": "GO",
        "productionAdoption": "NO_GO",
    }
    assert all(
        secret not in serialized for secret in ("token", "password", "credential", "secret")
    )
