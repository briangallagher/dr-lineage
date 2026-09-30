from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from openlineage.client.event_v2 import RunState
from openlineage.client.serde import Serde

from lineage_demo.kfp_lineage import (
    KFPLineageContext,
    KFPLineageContractError,
    KFPOutboxRecord,
    KFPRootEventProjector,
    KFPRunSnapshot,
    KFPStateHistoryEntry,
    root_event,
    task_event,
)

ROOT_ID = "42db060a-d89d-40d1-a7cb-28d6703b5d07"
TASK_ID = "a2fef4ea-9654-4e6d-9b64-53599bbf33cd"
SCHEMA_PATH = Path(__file__).parents[1] / "contracts/rhoai-kfp-lineage-context-1.0.schema.json"
ROOT_EVENT_SCHEMA_PATH = (
    Path(__file__).parents[1] / "contracts/rhoai-kfp-root-event-1.0.schema.json"
)
OUTBOX_SCHEMA_PATH = (
    Path(__file__).parents[1] / "contracts/rhoai-kfp-lineage-outbox-1.0.schema.json"
)


def context() -> KFPLineageContext:
    return KFPLineageContext(
        deployment="test-cluster",
        project="scenario-b",
        pipeline_id="7e44ad4e-f986-43c3-90d4-c6df8b73c783",
        pipeline_version_id="4707191e-5805-4314-8be6-87fadd84a7b9",
        pipeline_name="governed-asset-to-model",
        root_run_id=ROOT_ID,
    )


def test_context_envelope_is_versioned_and_contains_no_credentials() -> None:
    document = (
        context()
        .task(
            name="train-governed-baseline",
            task_id="train-governed-baseline",
            run_id=TASK_ID,
        )
        .document()
    )

    assert document == {
        "profile": "rhoai-kfp",
        "version": "1.0",
        "deployment": "test-cluster",
        "project": "scenario-b",
        "root": {
            "jobNamespace": "kfp://test-cluster/scenario-b",
            "jobName": "governed-asset-to-model",
            "runId": ROOT_ID,
        },
        "pipeline": {
            "id": "7e44ad4e-f986-43c3-90d4-c6df8b73c783",
            "versionId": "4707191e-5805-4314-8be6-87fadd84a7b9",
            "name": "governed-asset-to-model",
        },
        "parent": {
            "jobNamespace": "kfp://test-cluster/scenario-b",
            "jobName": "governed-asset-to-model",
            "runId": ROOT_ID,
        },
        "task": {
            "name": "train-governed-baseline",
            "id": "train-governed-baseline",
            "runId": TASK_ID,
            "attempt": 1,
        },
    }
    serialized = json.dumps(document).lower()
    assert all(secret not in serialized for secret in ("token", "password", "secret", "credential"))


def test_context_envelope_matches_the_versioned_schema() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    document = (
        context()
        .task(name="train-governed-baseline", task_id="train-governed-baseline", run_id=TASK_ID)
        .document()
    )

    assert list(validator.iter_errors(document)) == []


def test_context_reader_round_trips_a_task_without_logging_contents(tmp_path) -> None:
    document = (
        context()
        .task(name="train-governed-baseline", task_id="train-governed-baseline", run_id=TASK_ID)
        .document()
    )
    path = tmp_path / "context.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    parsed = KFPLineageContext.from_json_file(path)

    assert parsed.document() == document


def test_context_schema_requires_parent_when_task_is_present() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    document = (
        context()
        .task(
            name="train-governed-baseline",
            task_id="train-governed-baseline",
            run_id=TASK_ID,
        )
        .document()
    )
    del document["parent"]

    errors = list(validator.iter_errors(document))
    assert any("parent" in error.message for error in errors)


def test_context_reader_rejects_a_guessed_or_nested_parent() -> None:
    document = (
        context()
        .task(name="train-governed-baseline", task_id="train-governed-baseline", run_id=TASK_ID)
        .document()
    )
    document["parent"]["jobName"] = "unrelated-job"

    with pytest.raises(KFPLineageContractError, match="direct task parent"):
        KFPLineageContext.from_document(document)


def test_context_reader_rejects_a_root_job_mismatch() -> None:
    document = context().document()
    document["root"]["jobName"] = "unrelated-pipeline"

    with pytest.raises(KFPLineageContractError, match="root job"):
        KFPLineageContext.from_document(document)


def test_nested_task_context_points_to_the_immediate_parent_and_rejects_extra_fields() -> None:
    nested_run_id = "b4f2cf79-90dd-47f4-9d7f-6ebd44e9fd4b"
    nested = (
        context()
        .task(
            name="ingest",
            task_id="ingest",
            run_id=TASK_ID,
        )
        .task(
            name="transform",
            task_id="transform",
            run_id=nested_run_id,
        )
    )

    assert nested.parent_run_id == TASK_ID
    assert nested.document()["parent"]["runId"] == TASK_ID
    assert nested.run_facets()["parent"]["run"]["runId"] == TASK_ID
    document = context().document()
    document["unexpected"] = "must fail closed"

    with pytest.raises(KFPLineageContractError, match="unsupported fields"):
        KFPLineageContext.from_document(document)


def test_root_event_uses_exact_kfp_run_and_pipeline_version_context() -> None:
    event = root_event(context(), state=RunState.START, event_time="2026-09-30T12:00:00Z")
    payload = json.loads(Serde.to_json(event))

    assert payload["eventType"] == "START"
    assert payload["run"]["runId"] == ROOT_ID
    assert payload["job"] == {
        "namespace": "kfp://test-cluster/scenario-b",
        "name": "governed-asset-to-model",
        "facets": {},
    }
    assert payload["run"]["facets"]["rhoaiKfp"] == {
        "_producer": "https://github.com/briangallagher/dr-lineage/tree/main/sample-app-best-practices#v0.1.0",
        "_schemaURL": "urn:rhoai:kfp:lineage-context:1.0",
        "profile": "rhoai-kfp",
        "profileVersion": "1.0",
        "deployment": "test-cluster",
        "project": "scenario-b",
        "pipelineId": "7e44ad4e-f986-43c3-90d4-c6df8b73c783",
        "pipelineVersionId": "4707191e-5805-4314-8be6-87fadd84a7b9",
        "pipelineName": "governed-asset-to-model",
        "rootRunId": ROOT_ID,
    }


def test_root_event_matches_the_versioned_root_event_schema() -> None:
    schema = json.loads(ROOT_EVENT_SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    payload = json.loads(
        Serde.to_json(
            root_event(context(), state=RunState.COMPLETE, event_time="2026-09-30T12:00:00Z")
        )
    )

    assert list(validator.iter_errors(payload)) == []


def test_root_event_schema_rejects_fabricated_data_edges() -> None:
    schema = json.loads(ROOT_EVENT_SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    payload = json.loads(
        Serde.to_json(
            root_event(context(), state=RunState.START, event_time="2026-09-30T12:00:00Z")
        )
    )
    payload["inputs"].append({"namespace": "s3://unobserved", "name": "dataset"})

    errors = list(validator.iter_errors(payload))
    assert any(list(error.path) == ["inputs"] for error in errors)


def test_task_event_has_distinct_attempt_and_explicit_root_parent() -> None:
    task = context().task(
        name="train-governed-baseline",
        task_id="train-governed-baseline",
        run_id=TASK_ID,
        attempt=2,
    )
    payload = json.loads(
        Serde.to_json(
            task_event(
                task,
                state=RunState.FAIL,
                job_namespace="kfp://test-cluster/scenario-b",
                job_name="train-governed-baseline",
                event_time="2026-09-30T12:01:00Z",
            )
        )
    )

    assert payload["run"]["runId"] == TASK_ID
    assert payload["run"]["runId"] != ROOT_ID
    assert payload["run"]["facets"]["parent"] == {
        "_producer": "https://github.com/briangallagher/dr-lineage/tree/main/sample-app-best-practices#v0.1.0",
        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ParentRunFacet.json#/$defs/ParentRunFacet",
        "job": {"namespace": "kfp://test-cluster/scenario-b", "name": "governed-asset-to-model"},
        "run": {"runId": ROOT_ID},
        "root": {
            "job": {
                "namespace": "kfp://test-cluster/scenario-b",
                "name": "governed-asset-to-model",
            },
            "run": {"runId": ROOT_ID},
        },
    }
    assert payload["run"]["facets"]["rhoaiKfp"]["task"]["attempt"] == 2


def test_contract_rejects_non_uuid_root_and_reused_root_for_task() -> None:
    with pytest.raises(KFPLineageContractError, match="root_run_id must be a UUID"):
        KFPLineageContext(
            deployment="test-cluster",
            project="scenario-b",
            pipeline_id="pipeline-id",
            pipeline_version_id="version-id",
            pipeline_name="pipeline",
            root_run_id="pipeline/run/42",
        )

    with pytest.raises(KFPLineageContractError, match="task_run_id must differ"):
        context().task(
            name="train-governed-baseline",
            task_id="train-governed-baseline",
            run_id=ROOT_ID,
        )


def test_event_id_is_stable_for_replay_and_changes_for_retry_attempt() -> None:
    first = context().task(
        name="train-governed-baseline",
        task_id="train-governed-baseline",
        run_id=TASK_ID,
        attempt=1,
    )
    retry = context().task(
        name="train-governed-baseline",
        task_id="train-governed-baseline",
        run_id=str(uuid.uuid4()),
        attempt=2,
    )

    assert first.event_id(RunState.FAIL) == first.event_id(RunState.FAIL)
    assert first.event_id(RunState.FAIL) != retry.event_id(RunState.FAIL)


def snapshot(state: str, *, run_id: str = ROOT_ID) -> KFPRunSnapshot:
    return KFPRunSnapshot(
        run_id=run_id,
        pipeline_id="7e44ad4e-f986-43c3-90d4-c6df8b73c783",
        pipeline_version_id="4707191e-5805-4314-8be6-87fadd84a7b9",
        pipeline_name="governed-asset-to-model",
        state=state,
        event_time="2026-09-30T12:00:00Z",
    )


def test_root_projector_emits_start_and_terminal_once_with_replay_keys() -> None:
    projector = KFPRootEventProjector(context())

    first = projector.observe(snapshot("RUNNING"))
    assert [emission.idempotency_key for emission in first] == [
        "rhoai-kfp:1.0:42db060a-d89d-40d1-a7cb-28d6703b5d07:START",
        "rhoai-kfp:1.0:42db060a-d89d-40d1-a7cb-28d6703b5d07:RUNNING",
    ]
    assert [json.loads(Serde.to_json(emission.event))["eventType"] for emission in first] == [
        "START",
        "RUNNING",
    ]

    assert projector.observe(snapshot("RUNNING")) == ()
    terminal = projector.observe(snapshot("SUCCEEDED"))
    assert [json.loads(Serde.to_json(emission.event))["eventType"] for emission in terminal] == [
        "COMPLETE"
    ]
    assert projector.observe(snapshot("SUCCEEDED")) == ()


def test_root_projector_emits_start_before_a_first_observed_failure() -> None:
    projector = KFPRootEventProjector(context())

    emissions = projector.observe(snapshot("FAILED"))

    assert [json.loads(Serde.to_json(emission.event))["eventType"] for emission in emissions] == [
        "START",
        "FAIL",
    ]


def test_root_projector_preserves_kfp_state_history_timestamps() -> None:
    projector = KFPRootEventProjector(context())
    observed = snapshot("SUCCEEDED")
    observed = KFPRunSnapshot(
        run_id=observed.run_id,
        pipeline_id=observed.pipeline_id,
        pipeline_version_id=observed.pipeline_version_id,
        pipeline_name=observed.pipeline_name,
        state=observed.state,
        event_time=observed.event_time,
        state_history=(
            KFPStateHistoryEntry("PENDING", "2026-09-30T12:00:00Z"),
            KFPStateHistoryEntry("RUNNING", "2026-09-30T12:00:01Z"),
            KFPStateHistoryEntry("SUCCEEDED", "2026-09-30T12:01:00Z"),
        ),
    )

    emissions = projector.observe(observed)

    assert [json.loads(Serde.to_json(emission.event))["eventType"] for emission in emissions] == [
        "START",
        "RUNNING",
        "COMPLETE",
    ]
    assert [json.loads(Serde.to_json(emission.event))["eventTime"] for emission in emissions] == [
        "2026-09-30T12:00:00Z",
        "2026-09-30T12:00:01Z",
        "2026-09-30T12:01:00Z",
    ]


def test_root_projector_rejects_identity_mismatch_and_terminal_rewrite() -> None:
    projector = KFPRootEventProjector(context())

    with pytest.raises(KFPLineageContractError, match="does not match"):
        projector.observe(snapshot("RUNNING", run_id=TASK_ID))

    projector.observe(snapshot("SUCCEEDED"))
    with pytest.raises(KFPLineageContractError, match="cannot be rewritten"):
        projector.observe(snapshot("FAILED"))


def test_outbox_record_is_versioned_schema_valid_and_delivery_state_is_separate() -> None:
    schema = json.loads(OUTBOX_SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    emission = KFPRootEventProjector(context()).observe(snapshot("SUCCEEDED"))[-1]
    record = KFPOutboxRecord.from_emission(emission, created_at="2026-09-30T12:00:01Z")

    assert list(validator.iter_errors(record.document())) == []
    assert record.delivery_status == "PENDING"
    assert record.retry().document()["attempts"] == 1
    assert record.retry().delivery_status == "RETRY"
    assert record.delivered().delivery_status == "DELIVERED"
    assert record.dead_letter().delivery_status == "DEAD_LETTER"
    assert KFPOutboxRecord.from_document(record.document()) == record


def test_outbox_record_rejects_a_key_or_facet_mismatch() -> None:
    emission = KFPRootEventProjector(context()).observe(snapshot("SUCCEEDED"))[-1]
    document = KFPOutboxRecord.from_emission(emission, created_at="2026-09-30T12:00:01Z").document()
    document["idempotencyKey"] = document["idempotencyKey"].replace("COMPLETE", "FAIL")

    with pytest.raises(KFPLineageContractError, match="idempotency key"):
        KFPOutboxRecord.from_document(document)


def test_run_snapshot_rejects_unmapped_kfp_state() -> None:
    with pytest.raises(KFPLineageContractError, match="unsupported KFP run state"):
        snapshot("UNKNOWN")
