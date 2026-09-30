from __future__ import annotations

import copy
import io
import json
import uuid
from types import SimpleNamespace

import httpx
import pyarrow as arrow
import pyarrow.parquet as parquet
import pytest
from mlflow.tracking import MlflowClient
from openlineage.client.serde import Serde

from lineage_demo.config import Settings
from lineage_demo.events import LineageEmitter
from lineage_demo.governed_model import AssetReference, resolve_asset, run_governed_training
from lineage_demo.lifecycle import finish_root, start_root
from lineage_demo.verify_governed import GovernedContractError, verify_governed

ASSET_UUID = "2511fe7d-4892-4bf3-b440-0b395b497da2"
LOCATION = "s3://poc-underwriting/warehouse/forms/iso_form_extractions"


class RecordingClient:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(json.loads(Serde.to_json(event)))


class FakePaginator:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def paginate(self, *, Bucket: str, Prefix: str):
        assert Bucket == "poc-underwriting"
        assert Prefix == "warehouse/forms/iso_form_extractions/"
        yield {
            "Contents": [
                {
                    "Key": Prefix + "data/sample-00000.parquet",
                    "Size": len(self.payload),
                }
            ]
        }


class FakeS3:
    def __init__(self) -> None:
        stream = io.BytesIO()
        table = arrow.table({"line_of_business": ["commercial", "personal", "commercial"]})
        parquet.write_table(table, stream)
        self.payload = stream.getvalue()

    def get_paginator(self, operation: str):
        assert operation == "list_objects_v2"
        return FakePaginator(self.payload)

    def get_object(self, *, Bucket: str, Key: str):
        assert Bucket == "poc-underwriting"
        assert Key == "warehouse/forms/iso_form_extractions/data/sample-00000.parquet"
        return {
            "Body": io.BytesIO(self.payload),
            "ContentLength": len(self.payload),
            "ETag": '"mutable-etag"',
        }


def registry_client(
    *, location_field: str = "location", secret: str = "dataconnection-minio-iso-forms"
):
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.headers["X-Namespace"] == "scenario-b"
        assert request.url.path.endswith("/namespaces/forms/generic-tables/iso_form_extractions")
        return httpx.Response(
            200,
            json={
                "uuid": ASSET_UUID,
                "name": "iso_form_extractions",
                "collection": "forms",
                "asset_type": "table",
                "format": "parquet",
                location_field: LOCATION,
                "connection_ref": {"type": "rhai", "secret_name": secret},
                "owner": "sample-user",
                "updated_at": None,
            },
        )

    return httpx.Client(transport=httpx.MockTransport(respond))


@pytest.mark.parametrize("location_field", ["location", "storage_location"])
def test_resolve_live_and_contract_location_shapes_without_inventing_revision(
    location_field,
) -> None:
    with registry_client(location_field=location_field) as client:
        asset = resolve_asset(
            client,
            "https://registry.example",
            AssetReference("scenario-b", "forms", "iso_form_extractions", ASSET_UUID),
            "test-token",
            "dataconnection-minio-iso-forms",
        )
    assert asset.uuid == ASSET_UUID
    assert asset.location == LOCATION
    assert len(asset.metadata_sha256) == 64
    assert asset.response_etag is None


def test_wrong_connection_or_asset_uuid_fails_before_source_access() -> None:
    with registry_client() as client, pytest.raises(ValueError, match="Mounted Data Connection"):
        resolve_asset(
            client,
            "https://registry.example",
            AssetReference("scenario-b", "forms", "iso_form_extractions", ASSET_UUID),
            "test-token",
            "wrong-secret",
        )
    with registry_client() as client, pytest.raises(ValueError, match="UUID differs"):
        resolve_asset(
            client,
            "https://registry.example",
            AssetReference("scenario-b", "forms", "iso_form_extractions", str(uuid.uuid4())),
            "test-token",
            "dataconnection-minio-iso-forms",
        )


def test_registry_rejects_nonlocal_plain_http_before_sending_token() -> None:
    with registry_client() as client, pytest.raises(ValueError, match="TLS endpoint"):
        resolve_asset(
            client,
            "http://127.0.0.1.attacker.example",
            AssetReference("scenario-b", "forms", "iso_form_extractions", ASSET_UUID),
            "test-token",
            "dataconnection-minio-iso-forms",
        )


def test_asset_reference_rejects_path_segments() -> None:
    with pytest.raises(ValueError, match="invalid identifier"):
        AssetReference("scenario-b", "..", "iso_form_extractions", ASSET_UUID)


def test_local_model_path_records_mlflow_artifact_and_openlineage_evidence(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    token_file = tmp_path / "token"
    token_file.write_text("test-token", encoding="utf-8")
    ca_file = tmp_path / "service-ca.crt"
    ca_file.write_text("test-ca", encoding="utf-8")
    tracking_uri = (tmp_path / "mlruns").as_uri()
    recording = RecordingClient()
    settings = Settings(
        project_namespace="scenario-b",
        cluster_name="test-cluster",
        lineage_emit_attempts=1,
    )
    pipeline_job_id = "42db060a-d89d-40d1-a7cb-28d6703b5d07"
    emitter = LineageEmitter(settings, client=recording)
    start_root(settings, pipeline_job_id, emitter=emitter, job_name="governed-asset-to-model")
    with registry_client() as client:
        result = run_governed_training(
            settings=settings,
            registry_url="https://registry.example",
            registry_ca_file=str(ca_file),
            token_file=str(token_file),
            reference=AssetReference("scenario-b", "forms", "iso_form_extractions", ASSET_UUID),
            source_secret_name="dataconnection-minio-iso-forms",
            source_bucket="poc-underwriting",
            target_column="line_of_business",
            tracking_uri=tracking_uri,
            pipeline_job_id=pipeline_job_id,
            pod_name="train-governed-attempt-one",
            emitter=emitter,
            registry_client=client,
            s3=FakeS3(),
        )
    finish_root(
        settings,
        pipeline_job_id,
        "SUCCEEDED",
        emitter=emitter,
        job_name="governed-asset-to-model",
    )

    assert result["catalog_revision"] is None
    assert result["assurance"] == "OBSERVED"
    assert len(result["source_manifest_sha256"]) == 64
    assert result["evaluation"]["holdout_rows"] == 1
    mlflow_client = MlflowClient(tracking_uri=tracking_uri)
    run = mlflow_client.get_run(result["mlflow_run_id"])
    assert run.info.status == "FINISHED"
    assert run.data.tags["data_registry.asset_uuid"] == ASSET_UUID
    assert run.data.metrics["holdout_accuracy"] in (0.0, 1.0)
    assert "majority-classifier.json" in result["model_artifact_uri"]
    assert [item.path for item in mlflow_client.list_artifacts(run.info.run_id, "model")] == [
        "model/majority-classifier.json"
    ]
    assert [item.path for item in mlflow_client.list_artifacts(run.info.run_id, "evidence")] == [
        "evidence/observed-source.json"
    ]

    assert [event["eventType"] for event in recording.events] == [
        "START",
        "START",
        "COMPLETE",
        "COMPLETE",
    ]
    complete = recording.events[-2]
    assert complete["run"]["runId"] == result["openlineage_child_run_id"]
    assert complete["run"]["facets"]["parent"]["run"]["runId"] == (
        "42db060a-d89d-40d1-a7cb-28d6703b5d07"
    )
    assert complete["inputs"][0]["namespace"] == "dataregistry://test-cluster/scenario-b"
    assert complete["inputs"][0]["name"] == ASSET_UUID
    assert complete["inputs"][0]["facets"]["symlinks"]["identifiers"][0] == {
        "namespace": "s3://poc-underwriting",
        "name": "warehouse/forms/iso_form_extractions",
        "type": "TABLE",
    }
    assert complete["inputs"][1]["facets"]["version"]["datasetVersion"].startswith("sha256:")
    assert complete["outputs"][0]["name"].endswith("/model/majority-classifier.json")
    assert result["mlflow_run_id"] in complete["outputs"][0]["name"]

    kfp_run = SimpleNamespace(
        run_id=pipeline_job_id,
        state="SUCCEEDED",
        pipeline_version_reference=SimpleNamespace(
            pipeline_id="pipeline-id", pipeline_version_id="version-id"
        ),
    )
    verification = dict(
        result=result,
        events=recording.events,
        mlflow_client=mlflow_client,
        kfp_run=kfp_run,
        kfp_run_id=pipeline_job_id,
        pipeline_id="pipeline-id",
        pipeline_version_id="version-id",
        cluster="test-cluster",
        project="scenario-b",
        expected_asset_uuid=ASSET_UUID,
        expected_source_location=LOCATION,
    )
    checks = verify_governed(**verification)
    assert "source-evidence-artifact-matches-lineage" in checks
    assert "kfp-terminal-state-and-pipeline-version" in checks

    tampered_events = copy.deepcopy(recording.events)
    tampered_events[-2]["inputs"][1]["facets"]["version"]["datasetVersion"] = "sha256:" + "0" * 64
    with pytest.raises(GovernedContractError, match="source observations differ"):
        verify_governed(**{**verification, "events": tampered_events})

    extra_input_events = copy.deepcopy(recording.events)
    extra_input_events[-2]["inputs"].append(
        {"namespace": "s3://other-bucket", "name": "untracked.parquet"}
    )
    with pytest.raises(GovernedContractError, match="unexpected additional data inputs"):
        verify_governed(**{**verification, "events": extra_input_events})

    wrong_version = SimpleNamespace(
        **{
            **vars(kfp_run),
            "pipeline_version_reference": SimpleNamespace(
                pipeline_id="pipeline-id", pipeline_version_id="other-version"
            ),
        }
    )
    with pytest.raises(GovernedContractError, match="expected pipeline version"):
        verify_governed(**{**verification, "kfp_run": wrong_version})


@pytest.mark.parametrize(
    ("secret_name", "source_bucket", "message"),
    [
        ("wrong-secret", "poc-underwriting", "Mounted Data Connection"),
        ("dataconnection-minio-iso-forms", "other-bucket", "location bucket differs"),
        ("dataconnection-minio-iso-forms", "", "bucket is missing"),
    ],
)
def test_invalid_connection_fails_before_source_or_mlflow(
    tmp_path, secret_name, source_bucket, message
) -> None:
    token_file = tmp_path / "token"
    token_file.write_text("test-token", encoding="utf-8")
    ca_file = tmp_path / "ca"
    ca_file.write_text("test-ca", encoding="utf-8")
    recording = RecordingClient()
    settings = Settings(project_namespace="scenario-b", lineage_emit_attempts=1)
    with registry_client() as client, pytest.raises(ValueError, match=message):
        run_governed_training(
            settings=settings,
            registry_url="https://registry.example",
            registry_ca_file=str(ca_file),
            token_file=str(token_file),
            reference=AssetReference("scenario-b", "forms", "iso_form_extractions", ASSET_UUID),
            source_secret_name=secret_name,
            source_bucket=source_bucket,
            target_column="line_of_business",
            tracking_uri=(tmp_path / "mlruns").as_uri(),
            pipeline_job_id=str(uuid.uuid4()),
            pod_name="attempt-one",
            emitter=LineageEmitter(settings, client=recording),
            registry_client=client,
            s3=FakeS3(),
        )
    assert [event["eventType"] for event in recording.events] == ["START", "FAIL"]
    assert not (tmp_path / "mlruns").exists()
