# ruff: noqa: E501

"""Deterministic Feast-versus-Marquez OpenLineage conformance harness.

The harness deliberately tests semantics rather than only HTTP reachability.  It
posts the same small, deterministic event catalog to each configured backend,
reads the stored events and graph, and reports what is proven, missing, or not
observable through the deployed API.

The report is intentionally explanatory.  Each area includes a plain-language
description, why it matters, a significance rating, and a mitigation for a
failure or an architectural limit.  This makes the output useful as a gap
analysis as well as a CI-style verification result.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from lineage_demo import facets as standard_facets

STATUS_PROVEN = "PROVEN"
STATUS_FAILED = "FAILED"
STATUS_UNSUPPORTED = "UNSUPPORTED"
STATUS_NOT_OBSERVABLE = "NOT_OBSERVABLE"
STATUS_REQUIRES_EXTERNAL = "REQUIRES_EXTERNAL_INTEGRATION"

SEVERITIES = {"Critical", "High", "Medium", "Low"}


@dataclass(frozen=True)
class AreaSpec:
    """The explanation and risk context for one conformance area."""

    id: str
    title: str
    simple_meaning: str
    why_significant: str
    significance: str
    default_gap: str
    mitigation: str


@dataclass
class CheckResult:
    """One deterministic finding in the generated report."""

    id: str
    area: str
    status: str
    summary: str
    observed: Any = None
    expected: Any = None
    gap: str = ""
    mitigation: str = ""
    evidence: list[str] = field(default_factory=list)
    backend: str | None = None
    severity: str | None = None

    def as_dict(self) -> dict[str, Any]:
        area = AREA_SPECS[self.area]
        return {
            "id": self.id,
            "area": self.area,
            "title": area.title,
            "simple_meaning": area.simple_meaning,
            "why_significant": area.why_significant,
            "significance": self.severity or area.significance,
            "status": self.status,
            "summary": self.summary,
            "observed": self.observed,
            "expected": self.expected,
            "gap": self.gap or (area.default_gap if self.status != STATUS_PROVEN else ""),
            "mitigation": self.mitigation or area.mitigation,
            "evidence": self.evidence,
            "backend": self.backend,
        }


AREA_SPECS: dict[str, AreaSpec] = {
    "endpoint-and-ingestion": AreaSpec(
        id="endpoint-and-ingestion",
        title="Endpoint and event ingestion",
        simple_meaning="Can the backend receive the OpenLineage messages produced by the sample?",
        why_significant="If events cannot be accepted reliably, no downstream lineage answer exists.",
        significance="Critical",
        default_gap="The backend does not accept the required OpenLineage event contract.",
        mitigation="Pin a supported OpenLineage/Feast version, add producer compatibility tests, and use an outbox or retry policy for delivery failures.",
    ),
    "identity-and-symlinks": AreaSpec(
        id="identity-and-symlinks",
        title="Dataset identity and registry-to-physical linkage",
        simple_meaning="Does the backend know that a governed registry asset and the physical object used by a job are the same lineage subject?",
        why_significant="This is the central bridge between Data Registry assets and runtime ingestion. Without it, the graph is disconnected.",
        significance="Critical",
        default_gap="The backend stores the registry and physical identities but does not materialize their relationship.",
        mitigation="Make canonical namespace/name rules and SymlinksDatasetFacet emission a release contract; add a RHOAI projection if Feast only exposes generic graph primitives.",
    ),
    "execution-hierarchy": AreaSpec(
        id="execution-hierarchy",
        title="Execution hierarchy and data correlation",
        simple_meaning="Can users see both that data flowed between jobs and which pipeline execution owned each child run?",
        why_significant="Dataset edges alone do not explain one pipeline execution, retries, or component ownership.",
        significance="High",
        default_gap="The graph is data-connected but parent-run context is missing or not queryable.",
        mitigation="Require root context propagation and preserve ParentRunFacet; expose data edges and execution hierarchy as separate but linked views.",
    ),
    "lifecycle-and-retries": AreaSpec(
        id="lifecycle-and-retries",
        title="Lifecycle, failures, and retries",
        simple_meaning="Are START, terminal states, failures, and retry attempts retained without overwriting one another?",
        why_significant="Operational lineage is used to diagnose failed runs and understand whether a later success was a retry.",
        significance="High",
        default_gap="Lifecycle events are lost, contradictory states are silently collapsed, or retry identities are overwritten.",
        mitigation="Define duplicate and contradictory lifecycle policy, retain raw evidence, and use a RHOAI status projection for reconciled outcome versus reported events.",
    ),
    "facets-and-evidence": AreaSpec(
        id="facets-and-evidence",
        title="Facets and evidence semantics",
        simple_meaning="Does the backend preserve the metadata that explains ownership, schema, statistics, errors, symlinks, and evidence level?",
        why_significant="A graph without facets can show a path but cannot explain what happened or how trustworthy the claim is.",
        significance="High",
        default_gap="Standard or RHOAI-specific facets are dropped, redacted unexpectedly, or not available through queries.",
        mitigation="Maintain a versioned facet compatibility matrix; preserve unknown payloads safely and keep credentials and sensitive values out of facets.",
    ),
    "batch-and-delivery": AreaSpec(
        id="batch-and-delivery",
        title="Batch delivery and durability",
        simple_meaning="What happens when many events arrive together or the lineage service is temporarily unavailable?",
        why_significant="HTTP acceptance is not the same as durable delivery. Missing events create silent holes in operational history.",
        significance="High",
        default_gap="The API provides synchronous writes but no proven outbox, replay, dead-letter, or completeness contract.",
        mitigation="Use bounded retries plus a transactional outbox/dead-letter path, idempotent replay, and metrics for accepted, delayed, rejected, and reconciled events.",
    ),
    "authorization": AreaSpec(
        id="authorization",
        title="Authorization and project isolation",
        simple_meaning="Can one project read or assert another project's lineage?",
        why_significant="Lineage can expose physical locations, owners, error details, and relationships across governed assets.",
        significance="Critical",
        default_gap="The deployed endpoint accepts unauthenticated reads or writes, or namespace filtering is not bound to RHOAI authorization.",
        mitigation="Put the lineage API behind kube-rbac-proxy or an equivalent RHOAI gateway, bind producer identity to allowed namespaces, and test negative cross-project cases.",
    ),
    "retention": AreaSpec(
        id="retention",
        title="Retention, deletion, and historical answers",
        simple_meaning="How long do raw events and the graph remain, and can the system explain why an edge exists later?",
        why_significant="A current graph is not automatically a historical record. Incident and audit questions often need old run evidence.",
        significance="High",
        default_gap="Retention configuration is visible but pruning, archive, deletion, and graph-after-expiry behavior are not a tested contract.",
        mitigation="Define separate retention for raw events, runs, graph state, registry revisions, and audit records; add archive/restore and legal-hold behavior where required.",
    ),
    "recorrelation": AreaSpec(
        id="recorrelation",
        title="Late events, aliases, and re-correlation",
        simple_meaning="If a runtime event arrives before its registry alias, does the graph repair itself later?",
        why_significant="Independent producers and retries do not always arrive in order. Permanent disconnected nodes create misleading partial lineage.",
        significance="Medium",
        default_gap="Relationships are only created at ingest and late aliases remain disconnected.",
        mitigation="Add an audited, collision-safe re-correlation job or explicit alias mapping; never merge identities by fuzzy name matching.",
    ),
    "deployment-and-storage": AreaSpec(
        id="deployment-and-storage",
        title="Deployment topology, storage, and migrations",
        simple_meaning="Can the lineage backend be operated, upgraded, and scaled without destabilizing the feature registry or losing data?",
        why_significant="A semantically correct API is not a production backend if schema upgrades, pools, or workload isolation are unsafe.",
        significance="High",
        default_gap="The API does not prove connection isolation, migration safety, capacity, or restart/backup behavior.",
        mitigation="Use a separate lineage server/database profile where appropriate, versioned migrations, backup/restore tests, pool settings, and throughput/latency benchmarks.",
    ),
    "query-api": AreaSpec(
        id="query-api",
        title="Query API and RHOAI business semantics",
        simple_meaning="Can a client ask useful asset-centric questions, or does it only receive generic Feast/OpenLineage records?",
        why_significant="A graph primitive does not by itself answer ownership, revisions, impact, or why an asset is linked.",
        significance="High",
        default_gap="The generic API lacks Data Registry metadata, evidence labels, stable business IDs, or consistent partial-result semantics.",
        mitigation="Add a RHOAI lineage API/BFF that joins Feast to Data Registry metadata and authorization while keeping Feast's generic API available for native users.",
    ),
    "query-time-lineage": AreaSpec(
        id="query-time-lineage",
        title="Operational lineage versus query-time lineage",
        simple_meaning="Can users distinguish reported pipeline lineage from feature retrieval, serving, and request traces?",
        why_significant="Combining different evidence types into one graph can imply guarantees that no system actually provides.",
        significance="Medium",
        default_gap="The deployed OpenLineage API cannot prove integration with MLflow, OTel, or feature retrieval traces.",
        mitigation="Define separate operational, provenance/evidence, and runtime-trace planes with stable cross-links and explicit 'not instrumented' states.",
    ),
    "version-compatibility": AreaSpec(
        id="version-compatibility",
        title="Version and facet compatibility",
        simple_meaning="Does the exact RHOAI Feast build support the event versions and routes that producers rely on?",
        why_significant="Upstream Feast documentation is not proof of behavior in a pinned RHOAI image/operator combination.",
        significance="High",
        default_gap="The running API does not expose enough build identity to establish a supported Feast/operator/OpenLineage matrix.",
        mitigation="Record exact image digests and operator versions and run this suite as a release gate for every supported combination.",
    ),
    "ownership-and-revisions": AreaSpec(
        id="ownership-and-revisions",
        title="Data Registry ownership, revisions, and evidence",
        simple_meaning="Can the system distinguish a stable governed asset from a particular version of the data that was actually consumed?",
        why_significant="A stable asset UUID and a mutable object URI do not prove which bytes were used or provide reproducibility.",
        significance="Critical",
        default_gap="Feast can preserve a revision/evidence facet, but the authoritative Data Registry revision contract is outside this API test.",
        mitigation="Make Data Registry authoritative for revisions/evidence and carry versioned facets through Feast; expose assurance levels such as Linked, Observed, and Reproducible.",
    ),
}


def _deep_copy(value: Any) -> Any:
    return json.loads(json.dumps(value))


def _dataset(namespace: str, name: str, facets: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"namespace": namespace, "name": name, "facets": facets or {}}


def _run_event(
    *,
    event_time: str,
    event_type: str,
    run_id: str,
    job_namespace: str,
    job_name: str,
    run_facets: dict[str, Any] | None = None,
    job_facets: dict[str, Any] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "eventType": event_type,
        "eventTime": event_time,
        "producer": "https://github.com/openlineage/openlineage-conformance/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": run_id, "facets": run_facets or {}},
        "job": {"namespace": job_namespace, "name": job_name, "facets": job_facets or {}},
        "inputs": inputs or [],
        "outputs": outputs or [],
    }


def _job_event(
    *, event_time: str, job_namespace: str, job_name: str, facets: dict[str, Any]
) -> dict[str, Any]:
    return {
        "eventTime": event_time,
        "producer": "https://github.com/openlineage/openlineage-conformance/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
        "job": {"namespace": job_namespace, "name": job_name, "facets": facets},
        "inputs": [],
        "outputs": [],
    }


def _dataset_event(
    *, event_time: str, namespace: str, name: str, facets: dict[str, Any]
) -> dict[str, Any]:
    return {
        "eventTime": event_time,
        "producer": "https://github.com/openlineage/openlineage-conformance/1.0",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent",
        "dataset": {"namespace": namespace, "name": name, "facets": facets},
    }


@dataclass(frozen=True)
class Fixture:
    """A deterministic event catalog.  Change suite_id to isolate reruns."""

    suite_id: str

    @property
    def registry_namespace(self) -> str:
        return f"dataregistry://conformance/{self.suite_id}"

    @property
    def runtime_namespace(self) -> str:
        return f"dch://conformance/{self.suite_id}"

    @property
    def spark_namespace(self) -> str:
        return f"spark://conformance/{self.suite_id}"

    @property
    def pipeline_namespace(self) -> str:
        return f"kfp://conformance/{self.suite_id}"

    @property
    def physical_namespace(self) -> str:
        return f"s3://lineage-conformance/{self.suite_id}"

    @property
    def asset_name(self) -> str:
        return f"asset-{self.suite_id}"

    @property
    def raw_name(self) -> str:
        return "raw/documents.csv"

    @property
    def root_success(self) -> str:
        return f"00000000-0000-4000-8000-{self._digits('success')}"

    @property
    def root_failure(self) -> str:
        return f"00000000-0000-4000-8000-{self._digits('failure')}"

    def _digits(self, value: str) -> str:
        # UUID-shaped, stable IDs without using randomness or a timestamp.
        total = sum((index + 1) * ord(char) for index, char in enumerate(self.suite_id + value))
        return f"{total:012d}"[-12:]

    def _run(self, label: str, number: int) -> str:
        return f"{number:08d}-0000-4000-8000-{self._digits(label)}"

    def _job_facets(self, job_type: str) -> dict[str, Any]:
        return {
            "jobType": standard_facets.job_type("CONFORMANCE", job_type),
            "documentation": standard_facets.job_documentation("Deterministic conformance fixture"),
            "ownership": standard_facets.ownership("conformance"),
            "tags": standard_facets.tags({"purpose": "feast-vs-marquez", "suite": self.suite_id}),
            "sourceCodeLocation": standard_facets.source_code_location(
                "https://example.invalid/conformance", "fixture"
            ),
        }

    def _run_facets(self, parent: str | None = None, error: bool = False) -> dict[str, Any]:
        facets: dict[str, Any] = {
            "processing_engine": standard_facets.processing_engine("conformance", "1.0", "1.0")
        }
        if parent:
            if parent in {self.root_success, self.root_failure}:
                parent_namespace = self.pipeline_namespace
                parent_name = "conformance-root" if parent == self.root_success else "failed-root"
            else:
                parent_namespace = self.runtime_namespace
                parent_name = "read-and-stage"
            facets["parent"] = standard_facets.parent_run(
                parent_namespace=parent_namespace,
                parent_name=parent_name,
                parent_run_id=parent,
            )
        if error:
            facets["errorMessage"] = standard_facets.error_message("deterministic fixture failure")
        return facets

    def _out(self, name: str, *, rich: bool = True) -> dict[str, Any]:
        facets: dict[str, Any] = {}
        output_facets: dict[str, Any] = {}
        if rich:
            facets = {
                "schema": standard_facets.schema([{"name": "document", "type": "STRING"}]),
                "dataSource": standard_facets.data_source("sample-data", f"s3://sample-data/{self.suite_id}"),
                "storage": standard_facets.storage("S3", "CSV"),
                "datasetType": standard_facets.dataset_type("FILE"),
                "columnLineage": standard_facets.column_lineage(
                    {"document": ["document"]},
                    {"namespace": self.physical_namespace, "name": self.raw_name},
                ),
            }
            output_facets = {"outputStatistics": standard_facets.output_statistics(row_count=3, size=128, file_count=1)}
        return _dataset(self.physical_namespace, name, facets) | {"outputFacets": output_facets}

    def registry_event(self, *, event_time: str = "2026-01-01T00:00:00Z", name: str | None = None) -> dict[str, Any]:
        target = name or self.asset_name
        return _dataset_event(
            event_time=event_time,
            namespace=self.registry_namespace,
            name=target,
            facets={
                "documentation": standard_facets.documentation("Registered conformance asset"),
                "dataSource": standard_facets.data_source(
                    "sample-data", f"s3://sample-data/{self.suite_id}/{self.raw_name}"
                ),
                "datasetType": standard_facets.dataset_type("FILE"),
                "ownership": standard_facets.dataset_ownership("conformance"),
                "lifecycleStateChange": standard_facets.lifecycle_change("CREATE"),
                "symlinks": standard_facets.symlinks(self.physical_namespace, self.raw_name, "OBJECT"),
                "rhoaiEvidence": {
                    **standard_facets.base("RHOAIEvidenceDatasetFacet.json"),
                    "assetId": target,
                    "revisionId": "revision-001",
                    "assuranceLevel": "LINKED",
                },
            },
        )

    def events(self) -> list[dict[str, Any]]:
        root = self.root_success
        stage = f"staging/{self.suite_id}/documents.csv"
        transformed = f"transformed/{self.suite_id}/documents.parquet"
        embedding = f"embeddings/{self.suite_id}/vectors.jsonl"
        raw = _dataset(self.physical_namespace, self.raw_name)
        staged = _dataset(self.physical_namespace, stage)
        transformed_input = _dataset(self.physical_namespace, transformed)

        events: list[dict[str, Any]] = [self.registry_event()]
        events.append(
            _job_event(
                event_time="2026-01-01T00:00:01Z",
                job_namespace=self.pipeline_namespace,
                job_name="conformance-root",
                facets=self._job_facets("PIPELINE"),
            )
        )
        events.extend(
            [
                _run_event(
                    event_time="2026-01-01T00:00:02Z",
                    event_type="START",
                    run_id=root,
                    job_namespace=self.pipeline_namespace,
                    job_name="conformance-root",
                    run_facets=self._run_facets(),
                    job_facets=self._job_facets("PIPELINE"),
                ),
                _run_event(
                    event_time="2026-01-01T00:00:10Z",
                    event_type="COMPLETE",
                    run_id=root,
                    job_namespace=self.pipeline_namespace,
                    job_name="conformance-root",
                    run_facets=self._run_facets(),
                    job_facets=self._job_facets("PIPELINE"),
                ),
            ]
        )

        ingest = self._run("ingest", 10000001)
        events.extend(
            [
                _run_event(
                    event_time="2026-01-01T00:00:03Z",
                    event_type="START",
                    run_id=ingest,
                    job_namespace=self.runtime_namespace,
                    job_name="read-and-stage",
                    run_facets=self._run_facets(root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[raw],
                ),
                _run_event(
                    event_time="2026-01-01T00:00:04Z",
                    event_type="COMPLETE",
                    run_id=ingest,
                    job_namespace=self.runtime_namespace,
                    job_name="read-and-stage",
                    run_facets=self._run_facets(root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[raw],
                    outputs=[self._out(stage)],
                ),
            ]
        )

        spark = self._run("spark", 20000001)
        events.extend(
            [
                _run_event(
                    event_time="2026-01-01T00:00:05Z",
                    event_type="START",
                    run_id=spark,
                    job_namespace=self.spark_namespace,
                    job_name="transform-documents",
                    run_facets=self._run_facets(ingest),
                    job_facets=self._job_facets("APPLICATION"),
                    inputs=[staged],
                ),
                _run_event(
                    event_time="2026-01-01T00:00:06Z",
                    event_type="COMPLETE",
                    run_id=spark,
                    job_namespace=self.spark_namespace,
                    job_name="transform-documents",
                    run_facets=self._run_facets(ingest),
                    job_facets=self._job_facets("APPLICATION"),
                    inputs=[staged],
                    outputs=[self._out(transformed)],
                ),
            ]
        )

        embed = self._run("embed", 30000001)
        events.extend(
            [
                _run_event(
                    event_time="2026-01-01T00:00:07Z",
                    event_type="START",
                    run_id=embed,
                    job_namespace=self.runtime_namespace,
                    job_name="create-mock-embeddings",
                    run_facets=self._run_facets(root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[transformed_input],
                ),
                _run_event(
                    event_time="2026-01-01T00:00:08Z",
                    event_type="COMPLETE",
                    run_id=embed,
                    job_namespace=self.runtime_namespace,
                    job_name="create-mock-embeddings",
                    run_facets=self._run_facets(root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[transformed_input],
                    outputs=[self._out(embedding)],
                ),
            ]
        )

        failed_root = self.root_failure
        failed_attempt = self._run("failed-attempt-1", 40000001)
        retry_attempt = self._run("failed-attempt-2", 40000002)
        failure_output = f"failed/{self.suite_id}/attempt-1"
        retry_output = f"retry/{self.suite_id}/attempt-2"
        events.extend(
            [
                _run_event(
                    event_time="2026-01-01T00:01:00Z",
                    event_type="START",
                    run_id=failed_root,
                    job_namespace=self.pipeline_namespace,
                    job_name="failed-root",
                    run_facets=self._run_facets(),
                    job_facets=self._job_facets("PIPELINE"),
                ),
                _run_event(
                    event_time="2026-01-01T00:01:09Z",
                    event_type="FAIL",
                    run_id=failed_root,
                    job_namespace=self.pipeline_namespace,
                    job_name="failed-root",
                    run_facets=self._run_facets(error=True),
                    job_facets=self._job_facets("PIPELINE"),
                ),
                _run_event(
                    event_time="2026-01-01T00:01:01Z",
                    event_type="START",
                    run_id=failed_attempt,
                    job_namespace=self.runtime_namespace,
                    job_name="retryable-step",
                    run_facets=self._run_facets(failed_root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[raw],
                ),
                _run_event(
                    event_time="2026-01-01T00:01:02Z",
                    event_type="FAIL",
                    run_id=failed_attempt,
                    job_namespace=self.runtime_namespace,
                    job_name="retryable-step",
                    run_facets=self._run_facets(failed_root, error=True),
                    job_facets=self._job_facets("JOB"),
                    inputs=[raw],
                    outputs=[self._out(failure_output, rich=False)],
                ),
                _run_event(
                    event_time="2026-01-01T00:01:03Z",
                    event_type="START",
                    run_id=retry_attempt,
                    job_namespace=self.runtime_namespace,
                    job_name="retryable-step",
                    run_facets=self._run_facets(failed_root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[raw],
                ),
                _run_event(
                    event_time="2026-01-01T00:01:04Z",
                    event_type="COMPLETE",
                    run_id=retry_attempt,
                    job_namespace=self.runtime_namespace,
                    job_name="retryable-step",
                    run_facets=self._run_facets(failed_root),
                    job_facets=self._job_facets("JOB"),
                    inputs=[raw],
                    outputs=[self._out(retry_output, rich=False)],
                ),
            ]
        )
        return events

    def batch_events(self) -> list[dict[str, Any]]:
        return [
            _job_event(
                event_time="2026-01-01T00:02:00Z",
                job_namespace=self.runtime_namespace,
                job_name="batch-job",
                facets=self._job_facets("JOB"),
            ),
            _run_event(
                event_time="2026-01-01T00:02:01Z",
                event_type="COMPLETE",
                run_id=self._run("batch", 50000001),
                job_namespace=self.runtime_namespace,
                job_name="batch-job",
                run_facets=self._run_facets(),
                job_facets=self._job_facets("JOB"),
            ),
        ]

    def late_events(self) -> tuple[dict[str, Any], dict[str, Any]]:
        physical_name = "late/raw.csv"
        late_asset = f"late-{self.asset_name}"
        late_run = self._run("late", 60000001)
        runtime_event = _run_event(
            event_time="2026-01-01T00:03:00Z",
            event_type="COMPLETE",
            run_id=late_run,
            job_namespace=self.runtime_namespace,
            job_name="late-runtime-event",
            run_facets=self._run_facets(),
            job_facets=self._job_facets("JOB"),
            inputs=[_dataset(self.physical_namespace, physical_name)],
            outputs=[self._out("late/staged.csv", rich=False)],
        )
        registry_event = _dataset_event(
            event_time="2026-01-01T00:03:01Z",
            namespace=self.registry_namespace,
            name=late_asset,
            facets={
                "symlinks": standard_facets.symlinks(
                    self.physical_namespace, physical_name, "OBJECT"
                )
            },
        )
        return runtime_event, registry_event

    def dataset_names(self) -> list[str]:
        """Names needed when a backend exposes DatasetEvents separately."""
        return [self.asset_name, f"late-{self.asset_name}"]


def _extract_event(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    raw = item.get("event_json")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return item


def _event_key(event: dict[str, Any]) -> tuple[str, str, str]:
    if event.get("dataset"):
        dataset = event["dataset"]
        return ("dataset", dataset.get("namespace", ""), dataset.get("name", ""))
    job = event.get("job") or {}
    run = event.get("run") or {}
    return (event.get("eventType", ""), job.get("namespace", ""), run.get("runId", ""))


def _belongs_to_fixture(event: dict[str, Any], fixture: Fixture) -> bool:
    namespaces = {
        fixture.registry_namespace,
        fixture.runtime_namespace,
        fixture.spark_namespace,
        fixture.pipeline_namespace,
        fixture.physical_namespace,
    }
    if (event.get("dataset") or {}).get("namespace") in namespaces:
        return True
    job = event.get("job") or {}
    if job.get("namespace") in namespaces:
        return True
    run_id = (event.get("run") or {}).get("runId", "")
    return fixture.suite_id in run_id


def _event_datasets(event: dict[str, Any], key: str) -> set[tuple[str, str]]:
    return {
        (dataset.get("namespace", ""), dataset.get("name", ""))
        for dataset in event.get(key, [])
        if isinstance(dataset, dict)
    }


def _event_runs(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        run_id = (event.get("run") or {}).get("runId")
        if run_id:
            result.setdefault(run_id, []).append(event)
    return result


def _parent_id(event: dict[str, Any]) -> str:
    parent = ((event.get("run") or {}).get("facets") or {}).get("parent") or {}
    return ((parent.get("run") or {}).get("runId") or "")


def semantic_projection(events: list[dict[str, Any]]) -> dict[str, Any]:
    datasets: set[tuple[str, str]] = set()
    jobs: set[tuple[str, str]] = set()
    runs: dict[str, set[str]] = {}
    parents: set[tuple[str, str]] = set()
    symlinks: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    data_edges: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    facet_keys: dict[str, set[str]] = {}

    output_to_run: dict[tuple[str, str], str] = {}
    for event in events:
        job = event.get("job") or {}
        if job.get("namespace") and job.get("name"):
            jobs.add((job["namespace"], job["name"]))
            facet_keys.setdefault(f"job:{job['namespace']}:{job['name']}", set()).update(
                (job.get("facets") or {}).keys()
            )
        run = event.get("run") or {}
        run_id = run.get("runId")
        if run_id:
            runs.setdefault(run_id, set()).add(event.get("eventType", ""))
            facet_keys.setdefault(f"run:{run_id}", set()).update(
                (run.get("facets") or {}).keys()
            )
            parent = _parent_id(event)
            if parent:
                parents.add((run_id, parent))
        for direction in ("inputs", "outputs"):
            for identity in _event_datasets(event, direction):
                datasets.add(identity)
                if direction == "outputs" and run_id:
                    output_to_run[identity] = run_id
        dataset = event.get("dataset") or {}
        if dataset.get("namespace") and dataset.get("name"):
            identity = (dataset["namespace"], dataset["name"])
            datasets.add(identity)
            facets = dataset.get("facets") or {}
            facet_keys.setdefault(f"dataset:{identity[0]}:{identity[1]}", set()).update(
                facets.keys()
            )
            for identifier in (facets.get("symlinks") or {}).get("identifiers", []):
                target = (identifier.get("namespace", ""), identifier.get("name", ""))
                if target != ("", ""):
                    symlinks.add((identity, target))

    input_identities = set().union(*(_event_datasets(event, "inputs") for event in events)) if events else set()
    for event in events:
        for output in _event_datasets(event, "outputs"):
            for input_identity in input_identities:
                if output == input_identity:
                    data_edges.add((output, input_identity))

    return {
        "datasets": sorted(datasets),
        "jobs": sorted(jobs),
        "runs": {run_id: sorted(states) for run_id, states in sorted(runs.items())},
        "parents": sorted(parents),
        "symlinks": sorted(symlinks),
        "data_edges": sorted(data_edges),
        "facet_keys": {key: sorted(value) for key, value in sorted(facet_keys.items())},
    }


def _diff_projection(expected: dict[str, Any], observed: dict[str, Any]) -> dict[str, Any]:
    differences: dict[str, Any] = {}
    for key in expected:
        if key == "facet_keys":
            missing_facets: dict[str, list[str]] = {}
            observed_facets = observed.get(key, {})
            for entity, required in expected[key].items():
                missing = sorted(set(required) - set(observed_facets.get(entity, [])))
                if missing:
                    missing_facets[entity] = missing
            if missing_facets:
                differences[key] = {"missing": missing_facets}
            continue
        if expected[key] == observed.get(key):
            continue
        if isinstance(expected[key], list):
            exp = set(tuple(value) if isinstance(value, list) else value for value in expected[key])
            got = set(tuple(value) if isinstance(value, list) else value for value in observed.get(key, []))
            differences[key] = {
                "missing": sorted(exp - got, key=str),
                "unexpected": sorted(got - exp, key=str),
            }
        else:
            differences[key] = {"expected": expected[key], "observed": observed.get(key)}
    return differences


class BackendClient:
    """Small adapter over the deployed Feast or Marquez HTTP APIs."""

    def __init__(self, name: str, url: str, timeout: float = 30.0) -> None:
        self.name = name
        self.url = url.rstrip("/")
        self.client = httpx.Client(base_url=self.url, timeout=timeout)

    def close(self) -> None:
        self.client.close()

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        attempts = 3 if method.upper() == "GET" else 1
        for attempt in range(attempts):
            try:
                return self.client.request(method, path, **kwargs)
            except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError):
                if attempt == attempts - 1:
                    raise
                time.sleep(0.25 * (attempt + 1))
        raise AssertionError("unreachable")

    def post_event(self, event: dict[str, Any]) -> httpx.Response:
        path = "/api/v1/lineage" if self.name == "feast" else "/api/v1/lineage"
        return self.request("POST", path, json=event)

    def post_batch(self, events: list[dict[str, Any]]) -> httpx.Response:
        if self.name != "feast":
            return self.request("POST", "/api/v1/lineage/batch", json=events)
        return self.request("POST", "/api/v1/lineage/batch", json=events)

    def list_events(self, fixture: Fixture) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        offset = 0
        while True:
            if self.name == "feast":
                response = self.request(
                    "GET", "/api/v1/lineage/openlineage/events", params={"limit": 1000, "offset": offset}
                )
            else:
                response = self.request(
                    "GET", "/api/v1/events/lineage", params={"limit": 1000, "offset": offset, "sort": "asc"}
                )
            response.raise_for_status()
            body = response.json()
            page = body.get("events", []) if isinstance(body, dict) else []
            parsed = [_extract_event(item) for item in page]
            result.extend(event for event in parsed if event and _belongs_to_fixture(event, fixture))
            if len(page) < 1000:
                break
            offset += len(page)
        if self.name == "marquez":
            # Marquez's lineage-events endpoint returns RunEvents.  DatasetEvents
            # are materialized through the dataset entity API, so add those
            # entities to the normalized event view used by the comparison.
            for dataset_name in fixture.dataset_names():
                response = self.request(
                    "GET",
                    f"/api/v1/namespaces/{quote(fixture.registry_namespace, safe='')}/datasets/{quote(dataset_name, safe='')}",
                )
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                body = response.json()
                result.append(
                    {
                        "eventType": "OTHER",
                        "eventTime": "2026-01-01T00:00:00Z",
                        "producer": "marquez-dataset-entity",
                        "dataset": {
                            "namespace": body.get("namespace", fixture.registry_namespace),
                            "name": body.get("name", dataset_name),
                            "facets": body.get("facets", {}),
                        },
                    }
                )
        return result

    def native_graph(self, fixture: Fixture) -> dict[str, Any] | None:
        if self.name != "feast":
            return None
        response = self.request("GET", "/api/v1/lineage/openlineage/graph")
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            return None
        namespaces = {
            fixture.registry_namespace,
            fixture.runtime_namespace,
            fixture.spark_namespace,
            fixture.pipeline_namespace,
            fixture.physical_namespace,
        }
        nodes = [node for node in body.get("nodes", []) if node.get("namespace") in namespaces]
        names = {(node.get("type"), node.get("namespace"), node.get("name")) for node in nodes}
        edges = [
            edge
            for edge in body.get("edges", [])
            if not edge or any(value in names for value in _edge_node_values(edge))
        ]
        symlinks = [
            symlink
            for symlink in body.get("symlinks", [])
            if not symlink or any(value in names for value in _symlink_node_values(symlink))
        ]
        return {"nodes": nodes, "edges": edges, "symlinks": symlinks, "total_nodes": len(nodes)}


def _edge_node_values(edge: Any) -> list[tuple[str, str, str]]:
    if not isinstance(edge, dict):
        return []
    values: list[tuple[str, str, str]] = []
    for key in ("source", "target", "from", "to", "upstream", "downstream"):
        item = edge.get(key)
        if isinstance(item, dict):
            values.append((item.get("type", ""), item.get("namespace", ""), item.get("name", "")))
    for prefix in ("source", "target"):
        node = (
            edge.get(f"{prefix}_type", ""),
            edge.get(f"{prefix}_namespace", ""),
            edge.get(f"{prefix}_name", ""),
        )
        if any(node):
            values.append(node)
    return values


def _symlink_node_values(symlink: Any) -> list[tuple[str, str, str]]:
    if not isinstance(symlink, dict):
        return []
    return [
        ("dataset", symlink.get("dataset_namespace", ""), symlink.get("dataset_name", "")),
        ("dataset", symlink.get("linked_namespace", ""), symlink.get("linked_name", "")),
    ]


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text[:500]


def _result(
    check_id: str,
    area: str,
    status: str,
    summary: str,
    *,
    observed: Any = None,
    expected: Any = None,
    gap: str = "",
    mitigation: str = "",
    evidence: list[str] | None = None,
    backend: str | None = None,
    severity: str | None = None,
) -> CheckResult:
    if status not in {
        STATUS_PROVEN,
        STATUS_FAILED,
        STATUS_UNSUPPORTED,
        STATUS_NOT_OBSERVABLE,
        STATUS_REQUIRES_EXTERNAL,
    }:
        raise ValueError(f"Unknown conformance status: {status}")
    if severity and severity not in SEVERITIES:
        raise ValueError(f"Unknown severity: {severity}")
    return CheckResult(
        id=check_id,
        area=area,
        status=status,
        summary=summary,
        observed=observed,
        expected=expected,
        gap=gap,
        mitigation=mitigation,
        evidence=evidence or [],
        backend=backend,
        severity=severity,
    )


def _check_projection(
    backend: BackendClient, fixture: Fixture, events: list[dict[str, Any]], expected: dict[str, Any]
) -> CheckResult:
    observed = semantic_projection(events)
    differences = _diff_projection(expected, observed)
    return _result(
        "semantic-event-projection",
        "identity-and-symlinks",
        STATUS_PROVEN if not differences else STATUS_FAILED,
        "The backend retained the expected core identities and event-derived relationships."
        if not differences
        else "The backend did not retain the complete expected semantic projection.",
        observed=differences or "exact match",
        expected="exact match to deterministic fixture projection",
        gap=("Some OpenLineage identities, lifecycle states, parents, symlinks, or facets were lost." if differences else ""),
        mitigation=("Verify producer identity/version compatibility and add an adapter or Feast extension for the missing semantic fields." if differences else ""),
        evidence=[f"stored events returned from {backend.url}"],
        backend=backend.name,
    )


def _check_lifecycle(backend: BackendClient, fixture: Fixture, events: list[dict[str, Any]]) -> CheckResult:
    by_run = _event_runs(events)
    expected = {
        fixture.root_success: {"START", "COMPLETE"},
        fixture.root_failure: {"START", "FAIL"},
        fixture._run("failed-attempt-1", 40000001): {"START", "FAIL"},
        fixture._run("failed-attempt-2", 40000002): {"START", "COMPLETE"},
    }
    observed = {run_id: sorted({event.get("eventType", "") for event in by_run.get(run_id, [])}) for run_id in expected}
    failures = {
        run_id: {"expected": sorted(states), "observed": observed[run_id]}
        for run_id, states in expected.items()
        if set(observed[run_id]) != states
    }
    retry_outputs: set[str] = set()
    for run_id in (fixture._run("failed-attempt-1", 40000001), fixture._run("failed-attempt-2", 40000002)):
        for event in by_run.get(run_id, []):
            retry_outputs.update(dataset[1] for dataset in _event_datasets(event, "outputs"))
    if len(retry_outputs) != 2:
        failures["retry-output-identities"] = {"expected": 2, "observed": sorted(retry_outputs)}
    return _result(
        "lifecycle-and-retry-semantics",
        "lifecycle-and-retries",
        STATUS_PROVEN if not failures else STATUS_FAILED,
        "Lifecycle and retry identities were retained without collision." if not failures else "Lifecycle or retry semantics were not retained as expected.",
        observed={"runs": observed, "retry_outputs": sorted(retry_outputs), "failures": failures},
        expected={"runs": {key: sorted(value) for key, value in expected.items()}, "two distinct retry outputs": True},
        gap="The backend cannot be trusted to distinguish reported failure, retry, and success." if failures else "",
        mitigation="Preserve raw events and add an explicit lifecycle reconciliation policy." if failures else "",
        evidence=[f"run/event query for suite {fixture.suite_id}"],
        backend=backend.name,
    )


def _check_facets(backend: BackendClient, fixture: Fixture, events: list[dict[str, Any]]) -> CheckResult:
    registration = [
        event
        for event in events
        if (event.get("dataset") or {}).get("namespace") == fixture.registry_namespace
        and (event.get("dataset") or {}).get("name") == fixture.asset_name
    ]
    observed = sorted((registration[-1].get("dataset", {}).get("facets", {}) if registration else {}).keys())
    expected = ["dataSource", "datasetType", "documentation", "lifecycleStateChange", "ownership", "rhoaiEvidence", "symlinks"]
    missing = sorted(set(expected) - set(observed))
    return _result(
        "facet-preservation",
        "facets-and-evidence",
        STATUS_PROVEN if not missing else STATUS_FAILED,
        "Standard and RHOAI evidence facets survived the round trip." if not missing else "The backend dropped one or more required facets.",
        observed=observed,
        expected=expected,
        gap=f"Missing facets: {missing}" if missing else "",
        mitigation="Preserve unknown facet payloads and version any RHOAI-specific facet schema." if missing else "",
        evidence=["registry DatasetEvent round trip"],
        backend=backend.name,
    )


def _check_hierarchy(backend: BackendClient, fixture: Fixture, events: list[dict[str, Any]]) -> CheckResult:
    expected = {
        fixture._run("ingest", 10000001): fixture.root_success,
        fixture._run("embed", 30000001): fixture.root_success,
        fixture._run("spark", 20000001): fixture._run("ingest", 10000001),
        fixture._run("failed-attempt-1", 40000001): fixture.root_failure,
        fixture._run("failed-attempt-2", 40000002): fixture.root_failure,
    }
    observed: dict[str, str] = {}
    for event in events:
        child = (event.get("run") or {}).get("runId", "")
        parent = _parent_id(event)
        if child in expected and parent:
            observed[child] = parent
    missing = {run_id: parent for run_id, parent in expected.items() if observed.get(run_id) != parent}
    return _result(
        "parent-run-hierarchy",
        "execution-hierarchy",
        STATUS_PROVEN if not missing else STATUS_FAILED,
        "ParentRunFacet relationships were retained." if not missing else "Parent-run relationships were lost or changed.",
        observed=observed,
        expected=expected,
        gap="The data graph may remain connected while the user-level pipeline execution story is incomplete." if missing else "",
        mitigation="Require root context propagation and expose data correlation separately from execution hierarchy." if missing else "",
        evidence=["parent facet extracted from stored RunEvents"],
        backend=backend.name,
    )


def _check_api_surface(backend: BackendClient, fixture: Fixture) -> CheckResult:
    if backend.name == "feast":
        paths = [
            ("GET", "/openapi.json"),
            ("GET", "/api/v1/lineage/openlineage/events"),
            ("GET", "/api/v1/lineage/openlineage/jobs"),
            ("GET", "/api/v1/lineage/openlineage/datasets"),
            ("GET", "/api/v1/lineage/openlineage/graph"),
            ("GET", "/api/v1/lineage/openlineage/runs"),
            ("GET", f"/api/v1/lineage/openlineage/runs/{quote(fixture.root_success, safe='')}"),
            ("GET", "/api/v1/lineage/openlineage/retention"),
        ]
    else:
        paths = [("GET", "/api/v1/events/lineage"), ("GET", "/api/v1/namespaces")]
    observed: dict[str, int] = {}
    failures: dict[str, Any] = {}
    for method, path in paths:
        try:
            response = backend.request(method, path, params={"limit": 10} if "events" in path or path.endswith("runs") else None)
            observed[path] = response.status_code
            if response.status_code >= 400:
                failures[path] = {"status": response.status_code, "body": _safe_json(response)}
        except httpx.HTTPError as exc:
            failures[path] = str(exc)
    return _result(
        "query-api-surface",
        "query-api",
        STATUS_PROVEN if not failures else STATUS_FAILED,
        "The required generic query surface responded successfully." if not failures else "One or more required query endpoints failed.",
        observed=observed,
        expected={path: "2xx" for _, path in paths},
        gap="The generic API is incomplete or cannot retrieve the stored lineage entities." if failures else "",
        mitigation="Add a stable RHOAI lineage API/BFF and keep the Feast API behind a versioned compatibility contract." if failures else "",
        evidence=["OpenAPI-defined routes and live response status codes"],
        backend=backend.name,
    )


def _check_batch(backend: BackendClient, fixture: Fixture) -> CheckResult:
    events = fixture.batch_events()
    try:
        response = backend.post_batch(events)
        body = _safe_json(response)
        if response.status_code in {404, 405}:
            status = STATUS_UNSUPPORTED
            summary = "The backend has no batch-ingestion route."
        elif response.is_success:
            status = STATUS_PROVEN
            summary = "The backend accepted a deterministic batch of OpenLineage events."
        else:
            status = STATUS_FAILED
            summary = "The backend rejected a deterministic batch of OpenLineage events."
        return _result(
            "batch-ingestion",
            "batch-and-delivery",
            status,
            summary,
            observed={"status_code": response.status_code, "body": body},
            expected="2xx batch acceptance",
            gap="Batch delivery is unavailable or rejected." if status != STATUS_PROVEN else "",
            mitigation="Use a producer outbox and bounded single-event retries, or add a compatible batch endpoint." if status != STATUS_PROVEN else "",
            evidence=["POST /api/v1/lineage/batch"],
            backend=backend.name,
        )
    except httpx.HTTPError as exc:
        return _result(
            "batch-ingestion",
            "batch-and-delivery",
            STATUS_FAILED,
            "The batch request could not reach the backend.",
            observed=str(exc),
            expected="reachable batch endpoint",
            backend=backend.name,
        )


def _check_retention(backend: BackendClient) -> CheckResult:
    if backend.name != "feast":
        return _result(
            "retention-policy",
            "retention",
            STATUS_NOT_OBSERVABLE,
            "Marquez retention behavior is deployment-specific and is not exposed by the generic event API.",
            observed="no comparable retention endpoint",
            expected="documented raw-event, run-detail, graph, archive, and deletion semantics",
            backend=backend.name,
        )
    try:
        response = backend.request("GET", "/api/v1/lineage/openlineage/retention")
        body = _safe_json(response)
        if response.status_code >= 400:
            return _result(
                "retention-policy",
                "retention",
                STATUS_UNSUPPORTED,
                "Feast does not expose retention status on this deployment.",
                observed={"status_code": response.status_code, "body": body},
                expected="retention configuration/status endpoint",
                backend=backend.name,
            )
        return _result(
            "retention-policy",
            "retention",
            STATUS_NOT_OBSERVABLE,
            "Feast exposes retention configuration, but this run does not prove pruning, archive, restore, or graph-after-expiry behavior.",
            observed=body,
            expected="deterministic proof of retention and historical-answer semantics",
            gap="Configuration visibility is not a complete historical retention contract.",
            mitigation="Test pruning with an isolated database and define separate raw-event, run, graph, archive, and legal-hold policies.",
            evidence=["GET /api/v1/lineage/openlineage/retention"],
            backend=backend.name,
        )
    except httpx.HTTPError as exc:
        return _result("retention-policy", "retention", STATUS_FAILED, "Retention status could not be read.", observed=str(exc), backend=backend.name)


def _check_authorization(backend: BackendClient) -> CheckResult:
    try:
        response = backend.request("GET", "/api/v1/lineage/openlineage/events" if backend.name == "feast" else "/api/v1/events/lineage", params={"limit": 1})
        if response.status_code in {401, 403}:
            status = STATUS_PROVEN
            summary = "Unauthenticated lineage reads were rejected."
        elif response.status_code < 400:
            status = STATUS_FAILED
            summary = "The endpoint allowed an unauthenticated lineage read."
        else:
            status = STATUS_NOT_OBSERVABLE
            summary = "The endpoint response did not establish its authorization policy."
        return _result(
            "unauthenticated-read",
            "authorization",
            status,
            summary,
            observed={"status_code": response.status_code},
            expected="401 or 403 for an unauthenticated RHOAI lineage read",
            gap="The current endpoint is not enforcing a project-scoped read boundary." if status == STATUS_FAILED else "",
            mitigation="Protect the service with kube-rbac-proxy or an RHOAI gateway and add cross-project negative tests." if status == STATUS_FAILED else "",
            evidence=["GET lineage events without Authorization or X-API-Key"],
            backend=backend.name,
        )
    except httpx.HTTPError as exc:
        return _result("unauthenticated-read", "authorization", STATUS_FAILED, "Authorization probe could not reach the backend.", observed=str(exc), backend=backend.name)


def _check_native_graph(backend: BackendClient, fixture: Fixture) -> CheckResult:
    if backend.name != "feast":
        return _result(
            "native-graph-query",
            "query-api",
            STATUS_NOT_OBSERVABLE,
            "Marquez does not provide the same native graph endpoint; the harness compares its event-derived graph instead.",
            observed="event-derived comparison only",
            expected="backend graph query",
            backend=backend.name,
        )
    try:
        graph = backend.native_graph(fixture) or {}
        node_identities = {(node.get("namespace"), node.get("name")) for node in graph.get("nodes", [])}
        registry = (fixture.registry_namespace, fixture.asset_name)
        physical = (fixture.physical_namespace, fixture.raw_name)
        graph_links = {
            (
                link.get("dataset_namespace", ""),
                link.get("dataset_name", ""),
                link.get("linked_namespace", ""),
                link.get("linked_name", ""),
            )
            for link in graph.get("symlinks", [])
        }
        expected_link = (*registry, *physical)
        present = registry in node_identities and physical in node_identities and expected_link in graph_links
        return _result(
            "native-graph-query",
            "identity-and-symlinks",
            STATUS_PROVEN if present else STATUS_FAILED,
            "Feast's native graph exposes the registry node, physical node, and symlink relationship." if present else "Feast's native graph did not expose the complete registry-to-physical relationship.",
            observed={"nodes": len(graph.get("nodes", [])), "symlinks": graph.get("symlinks", []), "registry_and_physical_nodes": present},
            expected="registry and physical nodes plus symlink relationship",
            gap="The generic event store may contain the facet while the graph query fails to make the bridge usable." if not present else "",
            mitigation="Fix Feast graph indexing or add a RHOAI projection that resolves SymlinksDatasetFacet explicitly." if not present else "",
            evidence=["GET /api/v1/lineage/openlineage/graph"],
            backend=backend.name,
        )
    except httpx.HTTPError as exc:
        return _result("native-graph-query", "identity-and-symlinks", STATUS_FAILED, "Feast graph query failed.", observed=str(exc), backend=backend.name)


def _check_late_correlation(backend: BackendClient, fixture: Fixture) -> CheckResult:
    runtime_event, registry_event = fixture.late_events()
    try:
        first = backend.post_event(runtime_event)
        second = backend.post_event(registry_event)
        late_asset = f"late-{fixture.asset_name}"
        physical = (fixture.physical_namespace, "late/raw.csv")
        pair = ((fixture.registry_namespace, late_asset), physical)
        linked = False
        for _ in range(10):
            events = backend.list_events(fixture)
            projection = semantic_projection(events)
            linked = pair in projection["symlinks"]
            if backend.name == "feast":
                graph = backend.native_graph(fixture) or {}
                linked = linked and any(
                    link.get("dataset_namespace") == fixture.registry_namespace
                    and link.get("dataset_name") == late_asset
                    and link.get("linked_namespace") == physical[0]
                    and link.get("linked_name") == physical[1]
                    for link in graph.get("symlinks", [])
                )
            if linked:
                break
            time.sleep(0.25)
        status = STATUS_PROVEN if first.is_success and second.is_success and linked else STATUS_FAILED
        return _result(
            "late-event-recorrelation",
            "recorrelation",
            status,
            "A late registry alias is visible after the runtime event." if status == STATUS_PROVEN else "The late registry alias was not proven to repair the graph.",
            observed={"runtime_status": first.status_code, "registry_status": second.status_code, "symlink_visible": linked},
            expected="runtime event followed by registry symlink produces a visible relationship",
            gap="Late events may remain permanently disconnected from their governed asset." if status != STATUS_PROVEN else "",
            mitigation="Add an audited and collision-safe re-correlation job or explicit alias mapping." if status != STATUS_PROVEN else "",
            evidence=["runtime event posted before registry DatasetEvent", "post-ingest graph query"],
            backend=backend.name,
        )
    except httpx.HTTPError as exc:
        return _result("late-event-recorrelation", "recorrelation", STATUS_FAILED, "Late-event correlation test could not complete.", observed=str(exc), backend=backend.name)


def _not_observable(backend: BackendClient, check_id: str, area: str, summary: str, expected: str) -> CheckResult:
    return _result(
        check_id,
        area,
        STATUS_REQUIRES_EXTERNAL,
        summary,
        observed="not deterministically observable through the deployed HTTP API",
        expected=expected,
        backend=backend.name,
    )


def _post_fixture(backend: BackendClient, fixture: Fixture) -> tuple[list[dict[str, Any]], list[CheckResult]]:
    errors: list[dict[str, Any]] = []
    for index, event in enumerate(fixture.events()):
        try:
            response = backend.post_event(event)
            if not response.is_success:
                errors.append({"index": index, "status": response.status_code, "body": _safe_json(response)})
        except httpx.HTTPError as exc:
            errors.append({"index": index, "error": str(exc)})
    if errors:
        return [], [
            _result(
                "single-event-ingestion",
                "endpoint-and-ingestion",
                STATUS_FAILED,
                "One or more deterministic OpenLineage events were rejected.",
                observed=errors,
                expected="all fixture events accepted with 2xx responses",
                backend=backend.name,
            )
        ]
    return fixture.events(), [
        _result(
            "single-event-ingestion",
            "endpoint-and-ingestion",
            STATUS_PROVEN,
            "All deterministic OpenLineage events were accepted.",
            observed={"events_submitted": len(fixture.events())},
            expected={"events_submitted": len(fixture.events()), "responses": "2xx"},
            evidence=["POST /api/v1/lineage"],
            backend=backend.name,
        )
    ]


def run_backend(backend: BackendClient, fixture: Fixture) -> tuple[list[CheckResult], dict[str, Any]]:
    results: list[CheckResult] = []
    fixture_events, ingestion_results = _post_fixture(backend, fixture)
    results.extend(ingestion_results)
    if not fixture_events:
        results.extend(
            [
                _not_observable(backend, "semantic-event-projection", "identity-and-symlinks", "Semantic checks were skipped because ingestion failed.", "fixture projection"),
                _not_observable(backend, "lifecycle-and-retry-semantics", "lifecycle-and-retries", "Lifecycle checks were skipped because ingestion failed.", "START/terminal state and retry identities"),
            ]
        )
        return results, {"events": [], "projection": {}}

    try:
        stored_events = backend.list_events(fixture)
    except httpx.HTTPError as exc:
        results.append(_result("stored-event-query", "endpoint-and-ingestion", STATUS_FAILED, "Accepted events could not be read back.", observed=str(exc), backend=backend.name))
        return results, {"events": [], "projection": {}}

    expected_projection = semantic_projection(fixture_events)
    results.append(_check_projection(backend, fixture, stored_events, expected_projection))
    results.append(_check_hierarchy(backend, fixture, stored_events))
    results.append(_check_lifecycle(backend, fixture, stored_events))
    results.append(_check_facets(backend, fixture, stored_events))
    results.append(_check_batch(backend, fixture))
    results.append(_check_native_graph(backend, fixture))
    results.append(_check_api_surface(backend, fixture))
    results.append(_check_authorization(backend))
    results.append(_check_retention(backend))
    results.append(_check_late_correlation(backend, fixture))
    results.append(_not_observable(backend, "durability-contract", "batch-and-delivery", "The API test proves acceptance, not recovery after outage or durable replay.", "outbox, retry age, dead-letter, replay, and completeness behavior"))
    results.append(_not_observable(backend, "deployment-contract", "deployment-and-storage", "The API test cannot prove migrations, backups, connection pools, or capacity limits.", "restart, migration, backup/restore, and load-test evidence"))
    results.append(_not_observable(backend, "business-api-contract", "query-api", "Feast/Marquez generic endpoints do not by themselves prove the RHOAI asset-centric API.", "Data Registry metadata, owners, revisions, evidence, and policy-composed queries"))
    results.append(_not_observable(backend, "query-time-plane", "query-time-lineage", "The OpenLineage backend API does not prove feature retrieval or serving trace integration.", "stable links to MLflow/OTel/query-time lineage"))
    results.append(_not_observable(backend, "version-matrix", "version-compatibility", "The live API exposes route behavior but not a complete supported image/operator/OpenLineage matrix.", "exact Feast, operator, OpenLineage, and RHOAI compatibility evidence"))
    results.append(_not_observable(backend, "ownership-revision-contract", "ownership-and-revisions", "Facet round-trip is testable, but authoritative Data Registry revision and evidence semantics are external to this API.", "stable revisions, evidence levels, and reproducibility guarantees"))
    projection = semantic_projection(stored_events)
    return results, {"events": stored_events, "projection": projection}


def _compare_backends(snapshots: dict[str, dict[str, Any]]) -> CheckResult | None:
    if "feast" not in snapshots or "marquez" not in snapshots:
        return None
    feast = snapshots["feast"].get("projection", {})
    marquez = snapshots["marquez"].get("projection", {})
    # Facet keys are checked against the deterministic fixture for each backend.
    # Marquez may add derived facets such as ``sql`` and ``nominalTime``; those
    # are useful backend projections but are not a semantic requirement for the
    # Feast replacement comparison.
    core_marquez = {key: value for key, value in marquez.items() if key != "facet_keys"}
    core_feast = {key: value for key, value in feast.items() if key != "facet_keys"}
    differences = _diff_projection(core_marquez, core_feast)
    return _result(
        "feast-marquez-semantic-parity",
        "identity-and-symlinks",
        STATUS_PROVEN if not differences else STATUS_FAILED,
        "Feast and Marquez produced the same core semantic projection for the fixture." if not differences else "Feast and Marquez produced different core semantic projections.",
        observed=differences or "exact semantic match",
        expected="same identities, lifecycle states, parents, symlinks, data edges, and facet keys",
        gap="The backend migration changes lineage meaning for at least one tested contract." if differences else "",
        mitigation="Treat the difference as an intentional product decision or add an adapter/Feast extension before migration." if differences else "",
        evidence=["same deterministic event catalog posted to both backends"],
        backend="comparison",
        severity="Critical" if differences else None,
    )


def run_conformance(feast_url: str, marquez_url: str | None, suite_id: str, timeout: float = 30.0) -> dict[str, Any]:
    fixture = Fixture(suite_id=suite_id)
    urls = {"feast": feast_url}
    if marquez_url:
        urls["marquez"] = marquez_url
    snapshots: dict[str, dict[str, Any]] = {}
    results: list[CheckResult] = []
    clients: list[BackendClient] = []
    try:
        for name, url in urls.items():
            client = BackendClient(name, url, timeout=timeout)
            clients.append(client)
            try:
                results_for_backend, snapshot = run_backend(client, fixture)
            except httpx.HTTPError as exc:
                results_for_backend = [_result("backend-reachable", "endpoint-and-ingestion", STATUS_FAILED, "Backend could not be reached.", observed=str(exc), backend=name)]
                snapshot = {"events": [], "projection": {}}
            results.extend(results_for_backend)
            snapshots[name] = snapshot
        comparison = _compare_backends(snapshots)
        if comparison:
            results.append(comparison)
    finally:
        for client in clients:
            client.close()

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return {
        "schema_version": "1.0",
        "harness": "lineage_demo.conformance",
        "suite_id": suite_id,
        "fixture": {
            "deterministic": True,
            "event_catalog": "OpenLineage RunEvent, DatasetEvent, and JobEvent",
            "isolation": "Use a new suite_id for each run against a persistent backend; the harness does not reset shared lineage storage.",
        },
        "backends": {name: {"url": url} for name, url in urls.items()},
        "summary": counts,
        "results": [result.as_dict() for result in results],
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Feast versus Marquez conformance report",
        "",
        f"Suite: `{report['suite_id']}`  ",
        "Deterministic fixture: yes; shared backend data was not reset.",
        "",
        "## Summary",
        "",
    ]
    for status, count in sorted(report["summary"].items()):
        lines.append(f"- `{status}`: {count}")
    lines.extend(["", "## Findings", ""])
    for item in report["results"]:
        lines.extend(
            [
                f"### {item['id']} — {item['title']}",
                "",
                f"**Backend:** `{item.get('backend') or 'all'}`  ",
                f"**Status:** `{item['status']}`  ",
                f"**Significance:** `{item['significance']}`",
                "",
                f"**In simple terms:** {item['simple_meaning']}",
                "",
                f"**Why it matters:** {item['why_significant']}",
                "",
                f"**Result:** {item['summary']}",
                "",
            ]
        )
        if item.get("observed") is not None:
            lines.extend(["**Observed:**", "", "```json", json.dumps(item["observed"], indent=2, sort_keys=True, default=str), "```", ""])
        if item.get("expected") is not None:
            lines.extend([f"**Expected:** `{json.dumps(item['expected'], sort_keys=True, default=str)}`", ""])
        if item.get("gap"):
            lines.extend([f"**Gap ({item['significance']} significance):** {item['gap']}", ""])
        if item.get("mitigation"):
            lines.extend([f"**Potential mitigation:** {item['mitigation']}", ""])
        if item.get("evidence"):
            lines.extend([f"**Evidence:** {'; '.join(item['evidence'])}", ""])
    return "\n".join(lines).rstrip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feast-url", default=os.environ.get("FEAST_URL"), help="Feast lineage API base URL")
    parser.add_argument("--marquez-url", default=os.environ.get("MARQUEZ_URL"), help="Optional Marquez API base URL")
    parser.add_argument("--suite-id", default=os.environ.get("CONFORMANCE_SUITE_ID", "feast-vs-marquez"))
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--strict", action="store_true", help="Exit 1 when a finding is FAILED")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.feast_url:
        print("--feast-url or FEAST_URL is required", file=sys.stderr)
        return 2
    report = run_conformance(args.feast_url, args.marquez_url, args.suite_id, args.timeout)
    rendered = json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(rendered, encoding="utf-8")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(render_markdown(report), encoding="utf-8")
    print(f"Feast-versus-Marquez conformance suite: {report['suite_id']}")
    for status, count in sorted(report["summary"].items()):
        print(f"  {status}: {count}")
    if args.strict and report["summary"].get(STATUS_FAILED, 0):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
