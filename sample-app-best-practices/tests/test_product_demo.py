from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from lineage_demo.config import Settings
from lineage_demo.product_demo import build_product_demo, write_product_demo
from lineage_demo.replay_api import create_app

FIXTURE_ROOT = Path(__file__).parents[1]


def test_product_demo_builds_success_and_recovery_scenarios(tmp_path) -> None:
    document = build_product_demo(Settings(), FIXTURE_ROOT)
    result = write_product_demo(document, tmp_path / "demo", FIXTURE_ROOT)

    assert result["scenarioCount"] == 2
    assert result["eventCounts"] == {"happy-path": 9, "failure-retry": 11}
    assert result["verifiedEvidence"] == "VERIFIED_READ_ONLY"
    assert document["auditDossier"]["trustLevel"] == "VERIFIED_READ_ONLY_WITH_GAPS"
    assert document["nativeValidation"]["status"] == "PASS"
    assert document["nativeValidation"]["recommendation"] == "GO_FOR_NARROW_NATIVE_SPIKE"
    assert (
        document["decisionPackage"]["investmentRecommendation"]["decision"]
        == "FUND_BOUNDED_DISCOVERY"
    )
    assert len(document["decisionPackage"]["userJourneys"]) == 3
    assert len(document["decisionPackage"]["capabilityMatrix"]) == 7
    assert {item["id"] for item in document["nativeValidation"]["checks"]} == {
        "NATIVE-01",
        "NATIVE-02",
        "NATIVE-03",
        "NATIVE-04",
    }
    assert {item["system"] for item in document["auditDossier"]["systems"]} == {
        "KFP",
        "Data Registry",
        "MLflow",
        "Marquez/OpenLineage",
    }
    cockpit = Path(result["cockpit"]).read_text(encoding="utf-8")
    assert '<button class=\\"' in cockpit
    assert "function pickScenario" in cockpit
    brief = Path(result["pocBrief"]).read_text(encoding="utf-8")
    assert "Can we make governed asset-to-model traceability useful and explainable?" in brief
    assert "What the POC proves vs. what production requires" in brief
    assert "Native path checkpoint" in brief
    assert "GO_FOR_NARROW_NATIVE_SPIKE" in brief
    assert "DECISION REQUEST" in brief
    assert "Decision-ready governed asset-to-model demonstrator" in brief
    assert "Capability matrix" in brief
    assert "Architecture options and investment recommendation" in brief
    assert 'href="cockpit.html"' in brief
    assert 'href="decision-package.json"' in brief

    validation = json.loads(Path(tmp_path / "demo" / "native-path-validation.json").read_text())
    assert validation["evidenceState"] == "OBSERVED_LOCAL_CONTRACT_FIXTURE"
    assert all(item["status"] == "PASS" for item in validation["checks"])

    decision_package = json.loads(
        Path(tmp_path / "demo" / "decision-package.json").read_text(encoding="utf-8")
    )
    assert decision_package["schemaVersion"] == "rhoai-lineage-decision-package:1.0"
    assert (
        decision_package["investmentRecommendation"]["decision"]
        == "FUND_BOUNDED_DISCOVERY"
    )

    replay = json.loads(Path(result["replayData"]).read_text(encoding="utf-8"))
    retry = next(item for item in replay["scenarios"] if item["id"] == "failure-retry")
    failed = [
        event
        for event in retry["events"]
        if event["eventType"] == "FAIL" and event["job"]["name"] == "resolve-and-stage-data"
    ]
    assert len(failed) == 1
    assert failed[0]["run"]["facets"]["rhoaiKfp"]["task"]["attempt"] == 1
    assert retry["metadata"]["retry"]["successfulAttempt"] == 2
    assert retry["metadata"]["deliveryHistory"][-1]["deliveryStatus"] == "DEAD_LETTER"


def test_replay_api_exposes_scenarios_and_cockpit(tmp_path) -> None:
    document = build_product_demo(Settings(), FIXTURE_ROOT)
    write_product_demo(document, tmp_path / "demo", FIXTURE_ROOT)
    client = TestClient(create_app(tmp_path / "demo"))

    assert client.get("/api/health").json()["status"] == "ok"
    scenarios = client.get("/api/scenarios")
    assert scenarios.status_code == 200
    assert {item["id"] for item in scenarios.json()} == {"happy-path", "failure-retry"}
    assert client.get("/api/scenarios/failure-retry").status_code == 200
    assert client.get("/api/scenarios/missing").status_code == 404
    assert client.get("/api/dossier").json()["schemaVersion"] == "rhoai-audit-dossier:1.0"
    cockpit = client.get("/")
    assert "From governed data to an explainable model" in cockpit.text
    assert '<button class=\\"' in cockpit.text
    assert "Decision" in cockpit.text
    assert "decisionPackage" in cockpit.text
