"""Fail-fast verification of the OpenLineage event contract in Marquez."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from lineage_demo.identities import root_run_id

TERMINAL_STATES = {"COMPLETE", "FAIL", "ABORT"}
MANUAL_JOB_NAMES = {"read-and-stage", "create-mock-embeddings"}
REQUIRED_JOB_FACETS = {
    "jobType",
    "documentation",
    "ownership",
    "tags",
    "sourceCodeLocation",
}


class ContractError(AssertionError):
    pass


def load_events(marquez_url: str) -> list[dict[str, Any]]:
    response = httpx.get(
        f"{marquez_url.rstrip('/')}/api/v1/events/lineage",
        params={"limit": 10000, "sort": "asc"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("events", [])


def load_dataset_entity(
    marquez_url: str, namespace: str, name: str
) -> dict[str, Any]:
    """Load DatasetEvent state from Marquez's entity API.

    Marquez 0.50 persists DatasetEvents, but `/api/v1/events/lineage` returns
    RunEvents only. The materialized dataset endpoint is therefore the correct
    API for proving that registration facets were accepted and retained.
    """
    response = httpx.get(
        f"{marquez_url.rstrip('/')}/api/v1/namespaces/"
        f"{quote(namespace, safe='')}/datasets/{quote(name, safe='')}",
        timeout=30,
    )
    response.raise_for_status()
    entity = response.json()
    return {
        "dataset": {
            "namespace": entity.get("namespace", ""),
            "name": entity.get("name", ""),
            "facets": entity.get("facets", {}),
        }
    }


def _job(event: dict) -> tuple[str, str]:
    job = event.get("job", {})
    return job.get("namespace", ""), job.get("name", "")


def _run_id(event: dict) -> str:
    return event.get("run", {}).get("runId", "")


def _states(events: list[dict], run_id: str) -> set[str]:
    return {event.get("eventType", "") for event in events if _run_id(event) == run_id}


def _correlated_root_id(event: dict) -> str:
    parent = ((event.get("run") or {}).get("facets") or {}).get("parent") or {}
    root = (parent.get("root") or {}).get("run") or {}
    direct = parent.get("run") or {}
    return root.get("runId", "") or direct.get("runId", "")


def _identities(event: dict, field: str) -> set[tuple[str, str]]:
    return {(value.get("namespace", ""), value.get("name", "")) for value in event.get(field, [])}


def _assert_lifecycle(events: list[dict], run_id: str, expected_terminal: str) -> None:
    states = _states(events, run_id)
    if "START" not in states or states & TERMINAL_STATES != {expected_terminal}:
        raise ContractError(
            f"Run {run_id} expected START -> {expected_terminal}; observed {sorted(states)}"
        )


def _events_correlated_to(events: list[dict], root_id: str) -> list[dict]:
    return [event for event in events if _correlated_root_id(event) == root_id]


def _require_facets(event: dict, location: str, names: set[str]) -> None:
    facets = event.get(location, {}).get("facets", {})
    missing = names - facets.keys()
    if missing:
        raise ContractError(f"{_job(event)} event lacks {location} facets: {sorted(missing)}")


def verify(events: list[dict], report: dict, cluster: str, namespace: str) -> list[str]:
    checks: list[str] = []
    asset = report["asset"]
    registry_namespace = f"dataregistry://{cluster}/{namespace}"
    logical = (registry_namespace, asset["assetId"])
    physical = ("s3://sample-data", "raw/documents.csv")

    registration = [
        event
        for event in events
        if event.get("dataset", {}).get("namespace") == logical[0]
        and event.get("dataset", {}).get("name") == logical[1]
    ]
    if not registration:
        raise ContractError("No registry DatasetEvent was persisted")
    identifiers = (
        registration[-1]
        .get("dataset", {})
        .get("facets", {})
        .get("symlinks", {})
        .get("identifiers", [])
    )
    if physical not in {(item.get("namespace"), item.get("name")) for item in identifiers}:
        raise ContractError("Registry DatasetEvent does not symlink to the raw S3 identity")
    registration_facets = registration[-1].get("dataset", {}).get("facets", {})
    required_registration = {
        "documentation",
        "dataSource",
        "datasetType",
        "ownership",
        "lifecycleStateChange",
        "symlinks",
    }
    if missing := required_registration - registration_facets.keys():
        raise ContractError(f"Registry DatasetEvent lacks facets: {sorted(missing)}")
    checks.append("registry-symlink")
    checks.append("registry-facets")

    successful_roots = {
        root_run_id(item["run_id"])
        for item in report["runs"]
        if item["expected_state"] == "SUCCEEDED"
    }
    failed_roots = {
        root_run_id(item["run_id"]) for item in report["runs"] if item["expected_state"] == "FAILED"
    }
    for run_id in successful_roots:
        _assert_lifecycle(events, run_id, "COMPLETE")
    for run_id in failed_roots:
        _assert_lifecycle(events, run_id, "FAIL")
    root_events = [event for event in events if _run_id(event) in successful_roots | failed_roots]
    for event in root_events:
        _require_facets(event, "job", REQUIRED_JOB_FACETS)
        _require_facets(event, "run", {"processing_engine"})
        if event.get("eventType") == "FAIL":
            _require_facets(event, "run", {"errorMessage"})
    checks.append("root-lifecycles")
    checks.append("root-facets")

    expected_roots = successful_roots | failed_roots
    # Marquez retains events from earlier scenario suites.  Acceptance assertions
    # must evaluate the roots named in this report, rather than treating retained
    # history as if it belonged to the current suite.
    manual_children = [
        event
        for event in events
        if _job(event)[1] in MANUAL_JOB_NAMES
        and _correlated_root_id(event) in expected_roots
    ]
    for event in manual_children:
        _require_facets(event, "job", REQUIRED_JOB_FACETS)
        _require_facets(
            event,
            "run",
            {"parent", "executionParameters", "processing_engine"},
        )
        if event.get("eventType") in {"FAIL", "ABORT"}:
            _require_facets(event, "run", {"errorMessage"})
    for run_id in {_run_id(event) for event in manual_children}:
        terminal = _states(manual_children, run_id) & TERMINAL_STATES
        if "START" not in _states(manual_children, run_id) or len(terminal) != 1:
            raise ContractError(f"Manual child {run_id} lacks START and exactly one terminal state")
    checks.append("manual-parent-runs")
    checks.append("manual-lifecycles-and-facets")

    spark_events = [
        event
        for event in events
        if _job(event)[0].startswith("spark://")
        and _correlated_root_id(event) in expected_roots
    ]
    if not spark_events:
        raise ContractError("No events from the native Spark namespace were found")
    for event in spark_events:
        producer = event.get("producer", "")
        if "github.com/OpenLineage/OpenLineage" not in producer:
            raise ContractError(f"Spark event was not emitted by the native listener: {producer}")
    data_bearing_spark_events = [
        event for event in spark_events if event.get("inputs") or event.get("outputs")
    ]
    if not data_bearing_spark_events:
        raise ContractError("Native Spark events contain no data-bearing event")
    for event in data_bearing_spark_events:
        _require_facets(event, "run", {"parent", "processing_engine"})
        _require_facets(event, "job", {"jobType"})
    checks.append("native-spark-parent-runs")
    checks.append("native-spark-producer-and-facets")

    ingest_complete = [
        event
        for event in manual_children
        if _job(event)[1] == "read-and-stage" and event.get("eventType") == "COMPLETE"
    ]
    spark_inputs = set().union(*(_identities(event, "inputs") for event in spark_events))
    staged_outputs = set().union(*(_identities(event, "outputs") for event in ingest_complete))
    if not staged_outputs or not staged_outputs.issubset(spark_inputs):
        raise ContractError("Ingestion outputs do not exactly match Spark inputs")
    checks.append("ingest-to-spark-datasets")

    successful_spark_events = [
        event for event in spark_events if _correlated_root_id(event) in successful_roots
    ]
    spark_outputs = set().union(
        *(_identities(event, "outputs") for event in successful_spark_events)
    )
    embed_inputs = set().union(
        *(
            _identities(event, "inputs")
            for event in manual_children
            if _job(event)[1] == "create-mock-embeddings"
            and _correlated_root_id(event) in successful_roots
        )
    )
    transformed = {value for value in spark_outputs if value[1].startswith("transformed/")}
    if not transformed or not transformed.issubset(embed_inputs):
        raise ContractError("Spark transformed outputs do not exactly match embedding inputs")
    checks.append("spark-to-embedding-datasets")

    transformed_native_outputs = [
        output
        for event in spark_events
        for output in event.get("outputs", [])
        if output.get("name", "").startswith("transformed/")
    ]
    if not transformed_native_outputs or not any(
        "columnLineage" in output.get("facets", {}) for output in transformed_native_outputs
    ):
        raise ContractError("Native Spark transformed output lacks column lineage")
    checks.append("native-spark-column-lineage")

    for root_id in successful_roots:
        correlated = _events_correlated_to(events, root_id)
        root_ingests = [
            event
            for event in correlated
            if _job(event)[1] == "read-and-stage" and event.get("eventType") == "COMPLETE"
        ]
        root_sparks = [
            event
            for event in correlated
            if _job(event)[0].startswith("spark://") and event.get("eventType") == "COMPLETE"
        ]
        root_embeds = [
            event
            for event in correlated
            if _job(event)[1] == "create-mock-embeddings" and event.get("eventType") == "COMPLETE"
        ]
        if not root_ingests or not root_sparks or not root_embeds:
            raise ContractError(f"Successful root {root_id} lacks a completed processing stage")
        root_staged = set().union(*(_identities(event, "outputs") for event in root_ingests))
        root_spark_inputs = set().union(*(_identities(event, "inputs") for event in root_sparks))
        root_transformed = {
            value
            for event in root_sparks
            for value in _identities(event, "outputs")
            if value[1].startswith("transformed/")
        }
        root_embed_inputs = set().union(*(_identities(event, "inputs") for event in root_embeds))
        if not root_staged.issubset(root_spark_inputs):
            raise ContractError(f"Successful root {root_id} has a broken ingestion/Spark handoff")
        if not root_transformed or not root_transformed.issubset(root_embed_inputs):
            raise ContractError(f"Successful root {root_id} has a broken Spark/embedding handoff")
    checks.append("per-root-successful-stage-correlation")

    successful_ingest = [
        event for event in ingest_complete if _correlated_root_id(event) in successful_roots
    ]
    successful_embed = [
        event
        for event in manual_children
        if _job(event)[1] == "create-mock-embeddings"
        and event.get("eventType") == "COMPLETE"
        and _correlated_root_id(event) in successful_roots
    ]
    for event in successful_ingest + successful_embed:
        for output in event.get("outputs", []):
            dataset_missing = {"schema", "dataSource", "storage", "datasetType"} - output.get(
                "facets", {}
            ).keys()
            if dataset_missing:
                raise ContractError(
                    f"Output {_identities(event, 'outputs')} lacks dataset facets: "
                    f"{sorted(dataset_missing)}"
                )
            if "outputStatistics" not in output.get("outputFacets", {}):
                raise ContractError(f"Output {_identities(event, 'outputs')} lacks statistics")
    for event in successful_embed:
        if any("columnLineage" not in output.get("facets", {}) for output in event["outputs"]):
            raise ContractError("Embedding output lacks column lineage")
    checks.append("manual-output-facets")

    expected_jobs = {
        (f"dch-mock://{cluster}/{namespace}", "read-and-stage"),
        (f"kfp://{cluster}/{namespace}", "create-mock-embeddings"),
    }
    if {_job(event) for event in manual_children} != expected_jobs:
        raise ContractError("Manual processing jobs do not use stable canonical identities")
    output_names = [
        identity[1]
        for event in successful_ingest + successful_embed
        for identity in _identities(event, "outputs")
    ]
    if len(output_names) != len(set(output_names)):
        raise ContractError("Per-run manual output dataset names are not unique")
    checks.append("stable-jobs-distinct-runs-and-outputs")

    report_by_scenario = {item["scenario"]: item for item in report["runs"]}
    for failure_mode in ("ingest", "spark", "embed"):
        scenario = report_by_scenario.get(f"failure-{failure_mode}")
        if scenario is None:
            raise ContractError(f"Scenario report lacks failure-{failure_mode}")
        root_id = root_run_id(scenario["run_id"])
        correlated = _events_correlated_to(events, root_id)
        ingests = [event for event in correlated if _job(event)[1] == "read-and-stage"]
        sparks = [event for event in correlated if _job(event)[0].startswith("spark://")]
        embeds = [event for event in correlated if _job(event)[1] == "create-mock-embeddings"]
        if not ingests:
            raise ContractError(f"failure-{failure_mode} has no ingestion attempt")
        if failure_mode == "ingest":
            if any(event.get("eventType") == "COMPLETE" for event in ingests):
                raise ContractError("Injected ingestion failure unexpectedly completed")
            if sparks or embeds:
                raise ContractError("Ingestion failure did not suppress Spark and embedding")
            attempts = {_run_id(event) for event in ingests}
        elif failure_mode == "spark":
            if not any(event.get("eventType") == "COMPLETE" for event in ingests):
                raise ContractError("Spark failure scenario lacks completed ingestion")
            if not sparks or not any(event.get("eventType") == "FAIL" for event in sparks):
                raise ContractError("Injected Spark failure was not reported natively")
            for run_id in {_run_id(event) for event in sparks if event.get("eventType") == "FAIL"}:
                states = _states(sparks, run_id)
                if not {"START", "FAIL"}.issubset(states):
                    raise ContractError(
                        f"Native failed Spark run {run_id} lacks START and FAIL: {sorted(states)}"
                    )
            if embeds:
                raise ContractError("Spark failure did not suppress embedding")
            attempts = {
                _run_id(event)
                for event in sparks
                if event.get("job", {}).get("facets", {}).get("jobType", {}).get("jobType")
                == "APPLICATION"
            }
        else:
            if not any(event.get("eventType") == "COMPLETE" for event in ingests):
                raise ContractError("Embedding failure scenario lacks completed ingestion")
            if not sparks or not any(event.get("eventType") == "COMPLETE" for event in sparks):
                raise ContractError("Embedding failure scenario lacks completed Spark work")
            if not embeds or any(event.get("eventType") == "COMPLETE" for event in embeds):
                raise ContractError("Injected embedding failure unexpectedly completed")
            attempts = {_run_id(event) for event in embeds}
        if len(attempts) != 2:
            raise ContractError(
                f"failure-{failure_mode} expected two distinct task attempts; "
                f"observed {len(attempts)}"
            )
    checks.append("failure-transitions-and-downstream-suppression")
    checks.append("native-spark-start-and-fail-observed")
    checks.append("one-retry-with-distinct-child-runs")

    if report["bypass"]["eventCountBefore"] != report["bypass"]["eventCountAfter"]:
        raise ContractError("Bypass overwrite changed the lineage event count")
    checks.append("bypass-blindness-demonstrated")
    return checks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--marquez-url", required=True)
    parser.add_argument("--scenario-report", default="build/scenario-results.json")
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--namespace", default="ol-best-practices")
    args = parser.parse_args()
    report = json.loads(Path(args.scenario_report).read_text(encoding="utf-8"))
    registry_namespace = f"dataregistry://{args.cluster}/{args.namespace}"
    events = load_events(args.marquez_url)
    events.append(
        load_dataset_entity(
            args.marquez_url,
            registry_namespace,
            report["asset"]["assetId"],
        )
    )
    checks = verify(events, report, args.cluster, args.namespace)
    print(json.dumps({"status": "passed", "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
