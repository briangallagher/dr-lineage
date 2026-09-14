from __future__ import annotations

from fastapi.testclient import TestClient

from lineage_demo.config import Settings
from lineage_demo.events import LineageDeliveryError
from lineage_demo.registry_api import create_app
from lineage_demo.registry_store import FileRegistryStore


class RegistryEmitter:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls = []

    def dataset_event(self, identity, facets) -> None:
        self.calls.append((identity, facets))
        if self.fail:
            raise LineageDeliveryError("offline")


def build_client(tmp_path, emitter: RegistryEmitter) -> TestClient:
    settings = Settings(
        registry_store_backend="file",
        registry_store_path=str(tmp_path / "registry.json"),
        cluster_name="cluster",
        project_namespace="project",
    )
    app = create_app(
        settings=settings,
        store=FileRegistryStore(settings.registry_store_path),
        emitter=emitter,
    )
    return TestClient(app)


def request_body() -> dict:
    return {
        "name": "Documents",
        "description": "Mutable source",
        "owner": "team-a",
        "location": "s3://sample-data/raw/documents.csv",
    }


def test_registration_is_idempotent_and_emits_symlink(tmp_path) -> None:
    emitter = RegistryEmitter()
    client = build_client(tmp_path, emitter)
    first = client.post("/v1/assets", headers={"Idempotency-Key": "seed"}, json=request_body())
    second = client.post("/v1/assets", headers={"Idempotency-Key": "seed"}, json=request_body())
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["assetId"] == second.json()["assetId"]
    assert len(emitter.calls) == 1
    identity, emitted_facets = emitter.calls[0]
    assert identity.namespace == "dataregistry://cluster/project"
    assert emitted_facets["symlinks"]["identifiers"][0]["namespace"] == "s3://sample-data"


def test_record_is_durable_when_lineage_delivery_fails(tmp_path) -> None:
    emitter = RegistryEmitter(fail=True)
    client = build_client(tmp_path, emitter)
    response = client.post(
        "/v1/assets", headers={"Idempotency-Key": "pending"}, json=request_body()
    )
    assert response.status_code == 503
    assert response.json()["asset"]["lineageStatus"] == "PENDING"
    assets = client.get("/v1/assets").json()
    assert len(assets) == 1
    assert assets[0]["assetId"] == response.json()["asset"]["assetId"]


def test_patch_has_current_state_semantics(tmp_path) -> None:
    emitter = RegistryEmitter()
    client = build_client(tmp_path, emitter)
    asset = client.post(
        "/v1/assets", headers={"Idempotency-Key": "seed"}, json=request_body()
    ).json()
    updated = client.patch(
        f"/v1/assets/{asset['assetId']}",
        json={"location": "s3://sample-data/raw/replacement.csv"},
    )
    assert updated.status_code == 200
    assert updated.json()["location"].endswith("replacement.csv")
    assert len(emitter.calls) == 2
