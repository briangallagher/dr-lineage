from __future__ import annotations

import copy

import pytest

from lineage_demo.verify import ContractError, verify

CLUSTER = "cluster"
NAMESPACE = "ol-best-practices"
ROOTS = {
    "success-1": "00000000-0000-4000-8000-000000000001",
    "failure-ingest": "00000000-0000-4000-8000-000000000002",
    "failure-spark": "00000000-0000-4000-8000-000000000003",
    "failure-embed": "00000000-0000-4000-8000-000000000004",
}


def _job_facets() -> dict:
    return {
        "jobType": {},
        "documentation": {},
        "ownership": {},
        "tags": {},
        "sourceCodeLocation": {},
    }


def _manual_run_facets(root_id: str, *, error: bool = False) -> dict:
    result = {
        "parent": {"run": {"runId": root_id}},
        "executionParameters": {},
        "processing_engine": {},
    }
    if error:
        result["errorMessage"] = {}
    return result


def _root_event(root_id: str, state: str) -> dict:
    run_facets = {"processing_engine": {}}
    if state == "FAIL":
        run_facets["errorMessage"] = {}
    return {
        "eventType": state,
        "run": {"runId": root_id, "facets": run_facets},
        "job": {
            "namespace": f"kfp://{CLUSTER}/{NAMESPACE}",
            "name": "sample-app-best-practices",
            "facets": _job_facets(),
        },
    }


def _manual_event(
    *,
    root_id: str,
    run_id: str,
    name: str,
    state: str,
    inputs: list[dict] | None = None,
    outputs: list[dict] | None = None,
) -> dict:
    namespace = (
        f"dch-mock://{CLUSTER}/{NAMESPACE}"
        if name == "read-and-stage"
        else f"kfp://{CLUSTER}/{NAMESPACE}"
    )
    return {
        "eventType": state,
        "producer": "https://github.com/briangallagher/dr-lineage/tree/main/sample-app-best-practices#v0.1.0",
        "run": {
            "runId": run_id,
            "facets": _manual_run_facets(root_id, error=state in {"FAIL", "ABORT"}),
        },
        "job": {"namespace": namespace, "name": name, "facets": _job_facets()},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


def _spark_event(
    *,
    root_id: str,
    run_id: str,
    state: str,
    inputs: list[dict] | None = None,
    outputs: list[dict] | None = None,
) -> dict:
    run_facets = {
        "parent": {
            "run": {"runId": run_id},
            "root": {"run": {"runId": root_id}},
        },
        "processing_engine": {},
    }
    if state == "FAIL":
        run_facets["errorMessage"] = {}
    return {
        "eventType": state,
        "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.53.0/integration/spark",
        "run": {
            "runId": run_id,
            "facets": run_facets,
        },
        "job": {
            "namespace": f"spark://{CLUSTER}/{NAMESPACE}",
            "name": "transform-documents",
            "facets": {"jobType": {"jobType": "APPLICATION"}},
        },
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


def _identity(name: str) -> dict:
    return {"namespace": "s3://sample-data", "name": name, "facets": {}}


def _manual_output(name: str, *, column_lineage: bool = False) -> dict:
    facets = {"schema": {}, "dataSource": {}, "storage": {}, "datasetType": {}}
    if column_lineage:
        facets["columnLineage"] = {}
    return {
        "namespace": "s3://sample-data",
        "name": name,
        "facets": facets,
        "outputFacets": {"outputStatistics": {}},
    }


def _add_ingestion(events: list[dict], root: str, suffix: str, state: str = "COMPLETE") -> str:
    run_id = f"10000000-0000-4000-8000-0000000000{suffix}"
    stage = f"staging/asset/{run_id}/documents.csv"
    raw = _identity("raw/documents.csv")
    events.append(
        _manual_event(
            root_id=root, run_id=run_id, name="read-and-stage", state="START", inputs=[raw]
        )
    )
    events.append(
        _manual_event(
            root_id=root,
            run_id=run_id,
            name="read-and-stage",
            state=state,
            inputs=[raw],
            outputs=[_manual_output(stage)] if state == "COMPLETE" else [],
        )
    )
    return stage


def _add_spark(events: list[dict], root: str, suffix: str, stage: str, state: str) -> str:
    run_id = f"20000000-0000-4000-8000-0000000000{suffix}"
    transformed = f"transformed/asset/{run_id}"
    source = _identity(stage)
    events.append(_spark_event(root_id=root, run_id=run_id, state="START", inputs=[source]))
    outputs = []
    if state in {"COMPLETE", "FAIL"}:
        outputs = [
            {
                "namespace": "s3://sample-data",
                "name": transformed,
                "facets": {"columnLineage": {}},
                "outputFacets": {},
            }
        ]
    events.append(
        _spark_event(
            root_id=root,
            run_id=run_id,
            state=state,
            inputs=[source],
            outputs=outputs,
        )
    )
    return transformed


def _add_embedding(
    events: list[dict], root: str, suffix: str, transformed: str, state: str
) -> None:
    run_id = f"30000000-0000-4000-8000-0000000000{suffix}"
    source = _identity(transformed)
    events.append(
        _manual_event(
            root_id=root,
            run_id=run_id,
            name="create-mock-embeddings",
            state="START",
            inputs=[source],
        )
    )
    events.append(
        _manual_event(
            root_id=root,
            run_id=run_id,
            name="create-mock-embeddings",
            state=state,
            inputs=[source],
            outputs=(
                [_manual_output(f"embeddings/asset/{run_id}/vectors.jsonl", column_lineage=True)]
                if state == "COMPLETE"
                else []
            ),
        )
    )


def complete_contract() -> tuple[list[dict], dict]:
    registration = {
        "dataset": {
            "namespace": f"dataregistry://{CLUSTER}/{NAMESPACE}",
            "name": "asset",
            "facets": {
                "documentation": {},
                "dataSource": {},
                "datasetType": {},
                "ownership": {},
                "lifecycleStateChange": {},
                "symlinks": {
                    "identifiers": [
                        {
                            "namespace": "s3://sample-data",
                            "name": "raw/documents.csv",
                            "type": "OBJECT",
                        }
                    ]
                },
            },
        }
    }
    events = [registration]
    for scenario, root in ROOTS.items():
        events.extend(
            [
                _root_event(root, "START"),
                _root_event(root, "COMPLETE" if scenario == "success-1" else "FAIL"),
            ]
        )

    stage = _add_ingestion(events, ROOTS["success-1"], "11")
    transformed = _add_spark(events, ROOTS["success-1"], "11", stage, "COMPLETE")
    _add_embedding(events, ROOTS["success-1"], "11", transformed, "COMPLETE")

    _add_ingestion(events, ROOTS["failure-ingest"], "21", "FAIL")
    _add_ingestion(events, ROOTS["failure-ingest"], "22", "FAIL")

    stage = _add_ingestion(events, ROOTS["failure-spark"], "31")
    _add_spark(events, ROOTS["failure-spark"], "31", stage, "FAIL")
    _add_spark(events, ROOTS["failure-spark"], "32", stage, "FAIL")
    # OpenLineage Spark 1.53.0 was observed emitting COMPLETE just after FAIL
    # for the same failed data-bearing SQL run. Root state remains authoritative.
    events.append(
        _spark_event(
            root_id=ROOTS["failure-spark"],
            run_id="20000000-0000-4000-8000-000000000031",
            state="COMPLETE",
            inputs=[_identity(stage)],
        )
    )

    stage = _add_ingestion(events, ROOTS["failure-embed"], "41")
    transformed = _add_spark(events, ROOTS["failure-embed"], "41", stage, "COMPLETE")
    _add_embedding(events, ROOTS["failure-embed"], "41", transformed, "FAIL")
    _add_embedding(events, ROOTS["failure-embed"], "42", transformed, "FAIL")

    report = {
        "asset": {"assetId": "asset"},
        "bypass": {"eventCountBefore": 10, "eventCountAfter": 10},
        "runs": [
            {
                "scenario": scenario,
                "run_id": root,
                "expected_state": "SUCCEEDED" if scenario == "success-1" else "FAILED",
            }
            for scenario, root in ROOTS.items()
        ],
    }
    return events, report


def test_complete_scenario_contract_passes() -> None:
    events, report = complete_contract()
    checks = verify(events, report, CLUSTER, NAMESPACE)
    assert "failure-transitions-and-downstream-suppression" in checks
    assert "native-spark-column-lineage" in checks


def test_retained_events_from_an_older_suite_are_ignored() -> None:
    events, report = complete_contract()
    old_root = "00000000-0000-4000-8000-000000000099"
    events.extend(
        [
            _manual_event(
                root_id=old_root,
                run_id="30000000-0000-4000-8000-000000000099",
                name="create-mock-embeddings",
                state="START",
            ),
            _spark_event(
                root_id=old_root,
                run_id="20000000-0000-4000-8000-000000000099",
                state="FAIL",
            ),
        ]
    )

    verify(events, report, CLUSTER, NAMESPACE)


def test_missing_native_column_lineage_fails() -> None:
    events, report = complete_contract()
    broken = copy.deepcopy(events)
    for event in broken:
        if event.get("job", {}).get("namespace", "").startswith("spark://"):
            for output in event.get("outputs", []):
                output["facets"].pop("columnLineage", None)
    with pytest.raises(ContractError, match="column lineage"):
        verify(broken, report, CLUSTER, NAMESPACE)


def test_ingestion_failure_must_suppress_spark() -> None:
    events, report = complete_contract()
    events.append(
        _spark_event(
            root_id=ROOTS["failure-ingest"],
            run_id="20000000-0000-4000-8000-000000000099",
            state="START",
        )
    )
    with pytest.raises(ContractError, match="did not suppress"):
        verify(events, report, CLUSTER, NAMESPACE)
