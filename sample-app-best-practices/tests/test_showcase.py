from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator
from openlineage.client.serde import Serde

from lineage_demo.config import Settings
from lineage_demo.showcase import build_showcase, write_showcase

FIXTURE_ROOT = Path(__file__).parents[1]


def test_showcase_contains_a_product_level_governed_story(tmp_path) -> None:
    bundle = build_showcase(Settings())
    result = write_showcase(bundle, tmp_path / "showcase", FIXTURE_ROOT / "contracts")

    assert result["eventCount"] == 9
    assert result["emittedToMarquez"] == 0
    assert Path(result["auditReport"]).read_text(encoding="utf-8").startswith("<!doctype html>")

    events = json.loads(Path(result["events"]).read_text(encoding="utf-8"))
    assert events[0]["eventType"] == "START"
    assert events[-1]["eventType"] == "COMPLETE"
    assert any(event["job"]["name"] == "train-and-evaluate-model" for event in events)
    assert any(
        dataset["name"] == "2f66c8ef-4c4c-4a58-8c58-1c38a1e2d201"
        for event in events
        for dataset in event["inputs"]
    )
    train_complete = next(
        event
        for event in events
        if event["job"]["name"] == "train-and-evaluate-model" and event["eventType"] == "COMPLETE"
    )
    assert train_complete["run"]["facets"]["rhoaiEvaluation"]["decision"] == "CANDIDATE"
    assert train_complete["run"]["facets"]["rhoaiEvaluation"]["metrics"][0]["value"] == 0.92


def test_showcase_root_events_and_contexts_match_versioned_schemas(tmp_path) -> None:
    bundle = build_showcase(Settings())
    write_showcase(bundle, tmp_path / "showcase", FIXTURE_ROOT / "contracts")

    context_schema = json.loads(
        (FIXTURE_ROOT / "contracts/rhoai-kfp-lineage-context-1.0.schema.json").read_text()
    )
    root_schema = json.loads(
        (FIXTURE_ROOT / "contracts/rhoai-kfp-root-event-1.0.schema.json").read_text()
    )
    context_validator = Draft202012Validator(context_schema)
    root_validator = Draft202012Validator(root_schema)
    assert all(not list(context_validator.iter_errors(document)) for document in bundle.contexts)
    root_documents = [
        json.loads(Serde.to_json(event))
        for event in bundle.events
        if event.job.namespace.startswith("kfp://") and not event.inputs
    ]
    assert len(root_documents) == 3
    assert all(not list(root_validator.iter_errors(document)) for document in root_documents)
