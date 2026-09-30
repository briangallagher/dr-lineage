from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from lineage_demo.rhoai_kfp_qualification import (
    CONTRACT_CHECK_IDS,
    UPGRADE_SCENARIO_IDS,
    QualificationError,
    build_report,
    load_baseline,
)

FIXTURE = (
    Path(__file__).parents[1]
    / "examples"
    / "rhoai-kfp-qualification"
    / "rhoai-kfp-qualification-baseline-2026-09-30.json"
)


def test_release_pinned_baseline_validates_and_reports_explicit_no_go() -> None:
    document = load_baseline(FIXTURE)

    report = build_report(document)

    assert report["decision"] == "NO_GO"
    assert {check["id"] for check in report["checks"]} == {
        "G-00-release-mapping",
        "G-01-launcher-override",
        "G-02-launcher-runtime",
        "G-03-context-transport",
        "G-04-component-source-provenance",
        "G-05-owner-api",
    }
    assert next(
        check for check in report["checks"] if check["id"] == "G-03-context-transport"
    )["status"] == "blocked"
    assert next(
        check
        for check in report["checks"]
        if check["id"] == "G-04-component-source-provenance"
    )["status"] == "unknown"


def test_release_pinned_baseline_matches_published_json_schema() -> None:
    document = load_baseline(FIXTURE)
    schema_path = (
        Path(__file__).parents[1]
        / "contracts"
        / "rhoai-kfp-qualification-baseline-1.0.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    Draft202012Validator.check_schema(schema)
    errors = list(Draft202012Validator(schema).iter_errors(document))

    assert errors == []


def test_baseline_covers_all_contract_and_upgrade_checks() -> None:
    report = build_report(load_baseline(FIXTURE))

    assert {entry["id"] for entry in report["contractCoverage"]} == set(CONTRACT_CHECK_IDS)
    assert {entry["id"] for entry in report["upgradeScenarios"]} == set(UPGRADE_SCENARIO_IDS)
    assert all(entry["status"] in {"local-exercised", "native-blocked", "planned"}
               for entry in report["contractCoverage"])


@pytest.mark.parametrize("path_fragment", ["token", "password", "credential", "secret"])
def test_fixture_rejects_sensitive_field_names(tmp_path: Path, path_fragment: str) -> None:
    document = load_baseline(FIXTURE)
    document["unexpected_" + path_fragment] = "redacted"

    with pytest.raises(QualificationError, match="sensitive"):
        build_report(document)


def test_fixture_rejects_release_image_drift() -> None:
    document = load_baseline(FIXTURE)
    document["images"]["launcher"] = "registry.example/launcher@sha256:" + "0" * 64

    with pytest.raises(QualificationError, match="not pinned"):
        build_report(document)


def test_fixture_does_not_contain_json_credentials() -> None:
    serialized = json.dumps(load_baseline(FIXTURE)).lower()

    assert "bearer " not in serialized
    assert "access_key" not in serialized
    assert "secret_access_key" not in serialized
