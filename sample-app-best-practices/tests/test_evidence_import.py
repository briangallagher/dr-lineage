from __future__ import annotations

from lineage_demo.evidence_import import (
    normalize_kfp_run,
    normalize_marquez_events,
    normalize_mlflow_run,
    normalize_registry_asset,
    project_kfp_contract,
)

RUN_ID = "ad6efc7b-4b95-464e-b398-31df6b48e061"
PIPELINE_ID = "7e44ad4e-f986-43c3-90d4-c6df8b73c783"
VERSION_ID = "4707191e-5805-4314-8be6-87fadd84a7b9"


def test_registry_import_preserves_identity_and_safe_metadata() -> None:
    evidence = normalize_registry_asset(
        {
            "uuid": "33fbc314-ea15-4805-a958-95b8d29dd67d",
            "name": "iso_form_extractions",
            "collection": "forms",
            "asset_type": "table",
            "format": "parquet",
            "location": "s3://poc-underwriting/warehouse/forms/iso_form_extractions",
            "connection_ref": {"secret_name": "must-not-be-carried"},
        },
        endpoint="http://127.0.0.1:6572",
        project="scenario-b",
    )

    assert evidence["evidenceLevel"] == "VERIFIED_READ_ONLY"
    assert evidence["assetId"] == "33fbc314-ea15-4805-a958-95b8d29dd67d"
    assert evidence["project"] == "scenario-b"
    assert "secret_name" not in evidence


def test_kfp_import_allowlists_runtime_parameters_and_preserves_state_history() -> None:
    evidence = normalize_kfp_run(
        {
            "run_id": RUN_ID,
            "display_name": "governed-run",
            "pipeline_version_reference": {
                "pipeline_id": PIPELINE_ID,
                "pipeline_version_id": VERSION_ID,
            },
            "state": "SUCCEEDED",
            "state_history": [{"state": "PENDING", "update_time": "2026-09-30T09:00:00Z"}],
            "runtime_config": {
                "parameters": {
                    "project": "scenario-b",
                    "expected_asset_uuid": "33fbc314-ea15-4805-a958-95b8d29dd67d",
                    "source_secret_name": "must-not-be-carried",
                }
            },
            "run_details": {"task_details": []},
        },
        api_url="https://127.0.0.1:8888",
    )

    assert evidence["runId"] == RUN_ID
    assert evidence["pipelineVersionId"] == VERSION_ID
    assert evidence["runtimeParameters"] == {
        "project": "scenario-b",
        "expected_asset_uuid": "33fbc314-ea15-4805-a958-95b8d29dd67d",
    }
    assert evidence["stateHistory"][0]["state"] == "PENDING"


def test_imported_kfp_state_projects_through_context_root_and_outbox_contracts() -> None:
    projection = project_kfp_contract(
        {
            "runId": RUN_ID,
            "pipelineId": PIPELINE_ID,
            "pipelineVersionId": VERSION_ID,
            "displayName": "governed-run",
            "state": "SUCCEEDED",
            "stateHistory": [
                {"state": "PENDING", "eventTime": "2026-09-30T09:00:00Z"},
                {"state": "RUNNING", "eventTime": "2026-09-30T09:00:01Z"},
                {"state": "SUCCEEDED", "eventTime": "2026-09-30T09:01:00Z"},
            ],
        },
        deployment="test-cluster",
        project="scenario-b",
    )

    assert projection["evidenceLevel"] == "LOCAL_CONTRACT_PROJECTION"
    assert projection["context"]["root"]["runId"] == RUN_ID
    assert [event["eventType"] for event in projection["rootEvents"]] == [
        "START",
        "RUNNING",
        "COMPLETE",
    ]
    assert projection["outbox"][-1]["idempotencyKey"] == (
        f"rhoai-kfp:1.0:{RUN_ID}:COMPLETE"
    )


def test_marquez_import_handles_nullable_root_parent_and_correlates_children() -> None:
    evidence = normalize_marquez_events(
        {
            "events": [
                {"eventType": "START", "run": {"runId": RUN_ID, "facets": {"parent": None}}},
                {
                    "eventType": "COMPLETE",
                    "run": {
                        "runId": "child-run",
                        "facets": {"parent": {"run": {"runId": RUN_ID}}},
                    },
                },
                {"eventType": "START", "run": {"runId": "unrelated", "facets": {}}},
            ]
        },
        root_run_id=RUN_ID,
        endpoint="http://127.0.0.1:5000",
    )

    assert evidence["eventCount"] == 2
    assert evidence["states"] == ["COMPLETE", "START"]


def test_mlflow_import_requires_kfp_correlation_and_keeps_model_evidence_shape() -> None:
    evidence = normalize_mlflow_run(
        {
            "info": {
                "run_id": "8856dadf381141d4a5009c63d5c749b3",
                "experiment_id": "1",
                "run_name": "honorable-hare-566",
                "status": "FINISHED",
            },
            "data": {
                "metrics": [{"key": "holdout_accuracy", "value": 0.91, "step": 0}],
                "params": [{"key": "algorithm", "value": "majority-classifier"}],
                "tags": [
                    {"key": "kfp.root_run_id", "value": RUN_ID},
                    {"key": "data_registry.asset_uuid", "value": "asset-uuid"},
                    {"key": "service_token", "value": "must-not-survive"},
                ],
            },
        },
        endpoint="https://127.0.0.1:8443/mlflow",
        root_run_id=RUN_ID,
        artifacts={"files": [{"path": "model", "is_dir": True}]},
    )

    assert evidence["runId"] == "8856dadf381141d4a5009c63d5c749b3"
    assert evidence["metrics"][0]["key"] == "holdout_accuracy"
    assert evidence["artifacts"]["files"][0]["path"] == "model"
    assert "service_token" not in evidence["tags"]
