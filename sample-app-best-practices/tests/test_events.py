from __future__ import annotations

import json

import pytest
from openlineage.client.event_v2 import RunState
from openlineage.client.serde import Serde

from lineage_demo import facets
from lineage_demo.config import Settings
from lineage_demo.events import (
    LineageDeliveryError,
    LineageEmitter,
    input_dataset,
    output_dataset,
)
from lineage_demo.identities import DatasetIdentity


class RecordingClient:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.events = []
        self.calls = 0

    def emit(self, event) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transport unavailable")
        self.events.append(event)

    def close(self, _timeout: float) -> bool:
        return True


def settings(attempts: int = 3) -> Settings:
    return Settings(
        registry_store_backend="file",
        lineage_emit_attempts=attempts,
        lineage_emit_backoff_seconds=0,
    )


def test_dataset_event_serializes_spec_2_0_2_symlink() -> None:
    client = RecordingClient()
    emitter = LineageEmitter(settings(), client=client)
    emitter.dataset_event(
        DatasetIdentity("dataregistry://cluster/project", "asset-id"),
        {"symlinks": facets.symlinks("s3://sample-data", "raw/documents.csv", "OBJECT")},
    )
    payload = json.loads(Serde.to_json(client.events[0]))
    assert payload["schemaURL"].endswith("2-0-2/OpenLineage.json#/$defs/DatasetEvent")
    assert payload["dataset"]["facets"]["symlinks"]["identifiers"] == [
        {
            "namespace": "s3://sample-data",
            "name": "raw/documents.csv",
            "type": "OBJECT",
        }
    ]


def test_run_event_uses_input_and_output_dataset_types() -> None:
    client = RecordingClient()
    emitter = LineageEmitter(settings(), client=client)
    emitter.run_event(
        state=RunState.COMPLETE,
        run_id="01994c93-0748-77f1-aeb4-f31c5ef88543",
        job_namespace="test://jobs",
        job_name="job",
        inputs=[input_dataset(DatasetIdentity("s3://bucket", "input"))],
        outputs=[output_dataset(DatasetIdentity("s3://bucket", "output"))],
    )
    payload = json.loads(Serde.to_json(client.events[0]))
    assert payload["eventType"] == "COMPLETE"
    assert payload["inputs"][0]["name"] == "input"
    assert payload["outputs"][0]["name"] == "output"


def test_delivery_retries_then_succeeds() -> None:
    client = RecordingClient(failures=2)
    emitter = LineageEmitter(settings(), client=client, sleep=lambda _: None)
    emitter.dataset_event(DatasetIdentity("test", "dataset"), {})
    assert client.calls == 3


def test_delivery_failure_is_not_silenced() -> None:
    client = RecordingClient(failures=4)
    emitter = LineageEmitter(settings(), client=client, sleep=lambda _: None)
    with pytest.raises(LineageDeliveryError):
        emitter.dataset_event(DatasetIdentity("test", "dataset"), {})
