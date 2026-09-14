from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

from openlineage.client.event_v2 import (
    DatasetEvent,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
    StaticDataset,
)
from openlineage.client.serde import Serde

EVENTS = Path(__file__).parents[1] / "examples" / "events"
SCHEMA_PREFIX = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/"


def reconstruct(payload: dict):
    if "dataset" in payload:
        return DatasetEvent(
            eventTime=payload["eventTime"],
            producer=payload["producer"],
            dataset=StaticDataset(**payload["dataset"]),
        )
    run = payload["run"]
    job = payload["job"]
    return RunEvent(
        eventTime=payload["eventTime"],
        producer=payload["producer"],
        eventType=RunState(payload["eventType"]),
        run=Run(runId=run["runId"], facets=run.get("facets", {})),
        job=Job(namespace=job["namespace"], name=job["name"], facets=job.get("facets", {})),
        inputs=[InputDataset(**value) for value in payload.get("inputs", [])],
        outputs=[OutputDataset(**value) for value in payload.get("outputs", [])],
    )


def test_examples_conform_to_openlineage_2_0_2_generated_models() -> None:
    paths = sorted(EVENTS.glob("*.json"))
    assert len(paths) >= 4
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        event = reconstruct(payload)
        serialized = json.loads(Serde.to_json(event))
        expected_type = "DatasetEvent" if "dataset" in payload else "RunEvent"
        assert serialized["schemaURL"] == f"{SCHEMA_PREFIX}{expected_type}"
        if "run" in payload:
            uuid.UUID(serialized["run"]["runId"])


def test_all_example_facets_use_immutable_versioned_schema_urls() -> None:
    for path in EVENTS.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        containers = []
        if "dataset" in payload:
            containers.append(payload["dataset"].get("facets", {}))
        if "run" in payload:
            containers.append(payload["run"].get("facets", {}))
            containers.append(payload["job"].get("facets", {}))
        for facets in containers:
            for facet in facets.values():
                assert facet["_schemaURL"].startswith("https://openlineage.io/spec/facets/")
                assert re.search(r"/\d+-\d+-\d+/[^#]+\.json#/\$defs/", facet["_schemaURL"])
