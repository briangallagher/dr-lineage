# ruff: noqa: E501

"""Product-level KFP lineage showcase.

The showcase is deliberately deterministic and local.  It models the product
story we want to demonstrate in RHOAI while keeping the proposed KFP contract
visible underneath the friendly audit view:

    Data Registry asset -> KFP tasks -> model artifact
                         -> OpenLineage events -> Marquez

It does not claim that the current KFP deployment emits these events natively.
The ``--emit`` option sends the same event objects to the configured Marquez
endpoint when one is available.
"""

from __future__ import annotations

import html
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from openlineage.client.event_v2 import RunEvent, RunState
from openlineage.client.serde import Serde

from lineage_demo import facets
from lineage_demo.config import Settings
from lineage_demo.events import LineageEmitter, input_dataset, output_dataset
from lineage_demo.identities import DatasetIdentity
from lineage_demo.kfp_lineage import (
    KFPLineageContext,
    KFPOutboxRecord,
    KFPRootEventProjector,
    KFPRunSnapshot,
    KFPStateHistoryEntry,
    root_event,
    task_event,
)
from lineage_demo.lifecycle import root_job_facets
from lineage_demo.processing import _job_facets

SHOWCASE_ROOT_RUN_ID = "9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b001"
SHOWCASE_PIPELINE_ID = "7e44ad4e-f986-43c3-90d4-c6df8b73c783"
SHOWCASE_PIPELINE_VERSION_ID = "4707191e-5805-4314-8be6-87fadd84a7b9"
SHOWCASE_ASSET_ID = "2f66c8ef-4c4c-4a58-8c58-1c38a1e2d201"
SHOWCASE_MLFLOW_RUN_ID = "7f5a8a9e-2c55-4ec9-b2e8-0b5d7b0e3001"

EVENT_TIMES = {
    "pending": "2026-09-30T13:00:00Z",
    "running": "2026-09-30T13:00:02Z",
    "resolve-start": "2026-09-30T13:00:04Z",
    "resolve-complete": "2026-09-30T13:00:08Z",
    "prepare-start": "2026-09-30T13:00:09Z",
    "prepare-complete": "2026-09-30T13:00:18Z",
    "train-start": "2026-09-30T13:00:19Z",
    "train-complete": "2026-09-30T13:00:31Z",
    "complete": "2026-09-30T13:00:32Z",
}


@dataclass(frozen=True)
class ShowcaseBundle:
    """All deterministic artefacts produced by one showcase run."""

    context: KFPLineageContext
    contexts: tuple[dict[str, Any], ...]
    events: tuple[RunEvent, ...]
    outbox: tuple[dict[str, Any], ...]
    metadata: dict[str, Any]


def _json_event(event: RunEvent) -> dict[str, Any]:
    return json.loads(Serde.to_json(event))


def _dataset_facets(
    identity: DatasetIdentity, file_format: str, description: str
) -> dict[str, Any]:
    return {
        "documentation": facets.documentation(description),
        "dataSource": facets.data_source("RHOAI Data Registry", identity.namespace),
        "storage": facets.storage("S3", file_format),
        "datasetType": facets.dataset_type("FILE"),
    }


def _registry_asset_facets(logical: DatasetIdentity, physical: DatasetIdentity) -> dict[str, Any]:
    """Describe a registry asset without inventing a revision."""

    return {
        "documentation": facets.documentation(
            "Governed customer-risk training data registered in the RHOAI Data Registry."
        ),
        "dataSource": facets.data_source("RHOAI Data Registry", logical.namespace),
        "symlinks": facets.symlinks(physical.namespace, physical.name, "TABLE"),
        "ownership": facets.dataset_ownership("risk-data-product"),
        "rhoaiEvidence": {
            "_producer": facets.PRODUCER,
            "_schemaURL": "urn:rhoai:data-registry:evidence:1.0",
            "assetId": SHOWCASE_ASSET_ID,
            "assurance": "LINKED",
        },
    }


def _model_facets(model: DatasetIdentity) -> dict[str, Any]:
    return {
        "documentation": facets.documentation(
            "Candidate customer-risk model produced by the governed KFP training run."
        ),
        "dataSource": facets.data_source("MLflow", model.namespace),
        "storage": facets.storage("MLflow", "JSON"),
        "rhoaiModelLink": {
            "_producer": facets.PRODUCER,
            "_schemaURL": "urn:rhoai:mlflow:model-link:1.0",
            "trackingUri": "https://mlflow.example.invalid/rhoai",
            "runId": SHOWCASE_MLFLOW_RUN_ID,
            "artifactPath": "model/customer-risk.json",
        },
    }


def build_showcase(settings: Settings, root_run_id: str = SHOWCASE_ROOT_RUN_ID) -> ShowcaseBundle:
    """Build a deterministic root, task, data, and model lineage story."""

    context = KFPLineageContext(
        deployment=settings.cluster_name,
        project=settings.project_namespace,
        pipeline_id=SHOWCASE_PIPELINE_ID,
        pipeline_version_id=SHOWCASE_PIPELINE_VERSION_ID,
        pipeline_name="governed-customer-risk",
        root_run_id=root_run_id,
    )

    snapshot = KFPRunSnapshot(
        run_id=root_run_id,
        pipeline_id=SHOWCASE_PIPELINE_ID,
        pipeline_version_id=SHOWCASE_PIPELINE_VERSION_ID,
        pipeline_name="governed-customer-risk",
        state="SUCCEEDED",
        event_time=EVENT_TIMES["complete"],
        state_history=(
            KFPStateHistoryEntry("PENDING", EVENT_TIMES["pending"]),
            KFPStateHistoryEntry("RUNNING", EVENT_TIMES["running"]),
            KFPStateHistoryEntry("SUCCEEDED", EVENT_TIMES["complete"]),
        ),
    )
    projector = KFPRootEventProjector(context)
    root_emissions = projector.observe(snapshot)
    root_job = root_job_facets("governed-customer-risk")
    root_emissions = tuple(
        replace(
            emission,
            event=root_event(
                context,
                state=emission.event.eventType,
                job_facets=root_job,
                event_time=emission.event.eventTime,
            ),
        )
        for emission in root_emissions
    )
    root_events = [emission.event for emission in root_emissions]
    outbox = tuple(
        KFPOutboxRecord.from_emission(emission, created_at=EVENT_TIMES["pending"]).document()
        for emission in root_emissions
    )

    logical_asset = DatasetIdentity(
        f"dataregistry://{settings.cluster_name}/{settings.project_namespace}",
        SHOWCASE_ASSET_ID,
    )
    physical_asset = DatasetIdentity("s3://rhoai-showcase", "customer-risk/training.parquet")
    staged = DatasetIdentity("s3://rhoai-showcase", "staging/customer-risk/staged.parquet")
    features = DatasetIdentity("s3://rhoai-showcase", "features/customer-risk/features.parquet")
    model = DatasetIdentity(
        f"mlflow://{settings.cluster_name}/{settings.project_namespace}",
        "models/customer-risk/candidate",
    )

    physical_input = input_dataset(
        physical_asset,
        _dataset_facets(physical_asset, "PARQUET", "Observed source object resolved by the asset."),
    )
    logical_input = input_dataset(
        logical_asset, _registry_asset_facets(logical_asset, physical_asset)
    )
    staged_output = output_dataset(
        staged,
        _dataset_facets(staged, "PARQUET", "Validated and staged input for feature preparation."),
    )
    features_input = input_dataset(staged, _dataset_facets(staged, "PARQUET", "Staged source."))
    features_output = output_dataset(
        features,
        _dataset_facets(features, "PARQUET", "Feature table created by the preparation task."),
    )
    model_input = input_dataset(
        features, _dataset_facets(features, "PARQUET", "Training features.")
    )
    model_output = output_dataset(model, _model_facets(model))

    resolve = context.task(
        name="resolve-and-stage-data",
        task_id="resolve-and-stage-data",
        run_id="9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b002",
    )
    prepare = context.task(
        name="prepare-risk-features",
        task_id="prepare-risk-features",
        run_id="9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b003",
    )
    train = context.task(
        name="train-and-evaluate-model",
        task_id="train-and-evaluate-model",
        run_id="9d0d3bd9-cd56-4e8b-9f0b-6d04f2e4b004",
    )
    contexts = (resolve.document(), prepare.document(), train.document())
    evaluation_facets = {
        "rhoaiEvaluation": {
            "_producer": facets.PRODUCER,
            "_schemaURL": "urn:rhoai:mlflow:evaluation:1.0",
            "decision": "CANDIDATE",
            "metrics": [
                {"name": "holdoutAccuracy", "value": 0.92},
                {"name": "trainingRows", "value": 2400},
            ],
        }
    }

    task_events = [
        task_event(
            resolve,
            state=RunState.START,
            job_namespace=settings.ingest_namespace,
            job_name="resolve-and-stage-data",
            job_facets=_job_facets(
                integration="DCH",
                job_type_name="JOB",
                description="Resolve the governed Data Registry asset and stage its source.",
                path="src/lineage_demo/showcase.py",
            ),
            inputs=[logical_input, physical_input],
            event_time=EVENT_TIMES["resolve-start"],
        ),
        task_event(
            resolve,
            state=RunState.COMPLETE,
            job_namespace=settings.ingest_namespace,
            job_name="resolve-and-stage-data",
            job_facets=_job_facets(
                integration="DCH",
                job_type_name="JOB",
                description="Resolve the governed Data Registry asset and stage its source.",
                path="src/lineage_demo/showcase.py",
            ),
            inputs=[logical_input, physical_input],
            outputs=[staged_output],
            event_time=EVENT_TIMES["resolve-complete"],
        ),
        task_event(
            prepare,
            state=RunState.START,
            job_namespace=settings.spark_namespace,
            job_name="prepare-risk-features",
            job_facets=_job_facets(
                integration="SPARK",
                job_type_name="JOB",
                description="Prepare reproducible features for customer-risk training.",
                path="src/lineage_demo/showcase.py",
            ),
            inputs=[features_input],
            event_time=EVENT_TIMES["prepare-start"],
        ),
        task_event(
            prepare,
            state=RunState.COMPLETE,
            job_namespace=settings.spark_namespace,
            job_name="prepare-risk-features",
            job_facets=_job_facets(
                integration="SPARK",
                job_type_name="JOB",
                description="Prepare reproducible features for customer-risk training.",
                path="src/lineage_demo/showcase.py",
            ),
            inputs=[features_input],
            outputs=[features_output],
            event_time=EVENT_TIMES["prepare-complete"],
        ),
        task_event(
            train,
            state=RunState.START,
            job_namespace=settings.kfp_namespace,
            job_name="train-and-evaluate-model",
            job_facets=_job_facets(
                integration="KFP",
                job_type_name="JOB",
                description="Train and evaluate a candidate customer-risk model.",
                path="src/lineage_demo/showcase.py",
            ),
            inputs=[model_input],
            event_time=EVENT_TIMES["train-start"],
        ),
        task_event(
            train,
            state=RunState.COMPLETE,
            job_namespace=settings.kfp_namespace,
            job_name="train-and-evaluate-model",
            job_facets=_job_facets(
                integration="KFP",
                job_type_name="JOB",
                description="Train and evaluate a candidate customer-risk model.",
                path="src/lineage_demo/showcase.py",
            ),
            inputs=[model_input],
            outputs=[model_output],
            run_facets=evaluation_facets,
            event_time=EVENT_TIMES["train-complete"],
        ),
    ]

    events = tuple(root_events[:2] + task_events + root_events[2:])
    metadata = {
        "title": "Governed customer-risk model in RHOAI",
        "scenario": "A product team wants to understand which governed data produced a model.",
        "rootRunId": root_run_id,
        "pipeline": context.pipeline_name,
        "pipelineId": context.pipeline_id,
        "pipelineVersionId": context.pipeline_version_id,
        "project": settings.project_namespace,
        "marquezUrl": settings.marquez_url,
        "assetId": SHOWCASE_ASSET_ID,
        "model": model.name,
        "metrics": {"holdoutAccuracy": 0.92, "trainingRows": 2400},
        "eventCount": len(events),
        "contractProfile": "rhoai-kfp:1.0",
        "evidence": "DEMO_FIXTURE",
        "claims": [
            "The Data Registry asset is linked to the observed physical source.",
            "The KFP root owns orchestration context and lifecycle.",
            "Task producers report the data edges and model output they observed.",
            "The native KFP producer is represented by an executable local model, not claimed as deployed.",
        ],
    }
    return ShowcaseBundle(context, contexts, events, outbox, metadata)


def _validate_bundle(bundle: ShowcaseBundle, contracts_dir: Path) -> None:
    from jsonschema import Draft202012Validator

    context_schema = json.loads(
        (contracts_dir / "rhoai-kfp-lineage-context-1.0.schema.json").read_text(encoding="utf-8")
    )
    root_schema = json.loads(
        (contracts_dir / "rhoai-kfp-root-event-1.0.schema.json").read_text(encoding="utf-8")
    )
    context_validator = Draft202012Validator(context_schema)
    root_validator = Draft202012Validator(root_schema)
    for document in bundle.contexts:
        KFPLineageContext.from_document(document)
        errors = list(context_validator.iter_errors(document))
        if errors:
            raise ValueError(f"showcase context contract failed: {errors[0].message}")
    for document in bundle.outbox:
        KFPOutboxRecord.from_document(document)
    for event in bundle.events:
        document = _json_event(event)
        if document["job"]["namespace"].startswith("kfp://") and not document["inputs"]:
            errors = list(root_validator.iter_errors(document))
            if errors:
                raise ValueError(f"showcase root-event contract failed: {errors[0].message}")


def _event_rows(bundle: ShowcaseBundle) -> list[dict[str, str]]:
    rows = []
    for event in bundle.events:
        document = _json_event(event)
        rows.append(
            {
                "time": document["eventTime"],
                "state": document["eventType"],
                "job": document["job"]["name"],
                "run": document["run"]["runId"],
                "edges": f"{len(document['inputs'])} in / {len(document['outputs'])} out",
            }
        )
    return rows


def _render_html(bundle: ShowcaseBundle) -> str:
    metadata = bundle.metadata
    rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(row['time'])}</td>"
        f'<td><span class="state">{html.escape(row["state"])}</span></td>'
        f"<td>{html.escape(row['job'])}</td>"
        f"<td><code>{html.escape(row['run'][:8])}…</code></td>"
        f"<td>{html.escape(row['edges'])}</td>"
        "</tr>"
        for row in _event_rows(bundle)
    )
    claims = "\n".join(f"<li>{html.escape(claim)}</li>" for claim in metadata["claims"])
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(metadata["title"])}</title>
  <style>
    :root {{ color-scheme: light; --ink:#17212b; --muted:#5f6b76; --blue:#1769aa; --teal:#087f8c; --line:#d9e1e8; --soft:#f4f7fa; }}
    body {{ margin:0; font:15px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:var(--ink); background:#eef2f5; }}
    main {{ max-width:1120px; margin:0 auto; padding:40px 24px 64px; }}
    .hero {{ background:linear-gradient(120deg,#123b59,#126b75); color:white; border-radius:18px; padding:34px 38px; box-shadow:0 14px 32px #123b5926; }}
    .eyebrow {{ text-transform:uppercase; letter-spacing:.12em; font-size:12px; opacity:.8; }}
    h1 {{ font-size:34px; line-height:1.15; margin:10px 0 12px; }}
    h2 {{ margin:0 0 14px; font-size:22px; }}
    .hero p {{ max-width:760px; margin:0; font-size:17px; opacity:.92; }}
    .grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin:22px 0; }}
    .card, section {{ background:white; border:1px solid var(--line); border-radius:14px; padding:20px; box-shadow:0 3px 10px #17324d0b; }}
    .metric {{ font-size:24px; font-weight:700; color:var(--blue); }}
    .label {{ color:var(--muted); font-size:13px; }}
    section {{ margin-top:18px; }}
    .flow {{ display:flex; align-items:stretch; gap:10px; overflow:auto; padding:8px 0 4px; }}
    .node {{ min-width:180px; background:var(--soft); border:1px solid var(--line); border-radius:12px; padding:14px; }}
    .node strong {{ display:block; margin-bottom:5px; }}
    .arrow {{ display:flex; align-items:center; color:var(--teal); font-size:24px; }}
    .tag {{ display:inline-block; border-radius:999px; padding:4px 9px; margin:2px 4px 2px 0; background:#e5f4f4; color:#075b64; font-size:12px; font-weight:600; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; }}
    th,td {{ text-align:left; padding:10px 8px; border-bottom:1px solid var(--line); vertical-align:top; }}
    th {{ color:var(--muted); font-weight:600; }}
    .state {{ font-weight:700; color:var(--teal); }}
    code {{ background:var(--soft); padding:2px 5px; border-radius:4px; }}
    .note {{ color:var(--muted); }}
    a {{ color:var(--blue); }}
    @media (max-width:800px) {{ .grid {{ grid-template-columns:repeat(2,1fr); }} h1 {{ font-size:28px; }} }}
  </style>
</head>
<body><main>
  <div class="hero">
    <div class="eyebrow">RHOAI product showcase · KFP + OpenLineage + Data Registry</div>
    <h1>{html.escape(metadata["title"])}</h1>
    <p>{html.escape(metadata["scenario"])} The audit trail answers which governed asset was used, which pipeline ran, and which model was produced.</p>
  </div>
  <div class="grid">
    <div class="card"><div class="metric">{html.escape(metadata["pipeline"])}</div><div class="label">KFP pipeline</div></div>
    <div class="card"><div class="metric">{html.escape(str(metadata["eventCount"]))}</div><div class="label">contract-checked events</div></div>
    <div class="card"><div class="metric">{html.escape(metadata["contractProfile"])}</div><div class="label">lineage profile</div></div>
    <div class="card"><div class="metric">{html.escape(str(metadata["metrics"]["holdoutAccuracy"]))}</div><div class="label">holdout accuracy</div></div>
  </div>
  <section>
    <h2>The product view</h2>
    <div class="flow">
      <div class="node"><strong>Governed dataset</strong><span class="tag">Data Registry</span><br><code>{html.escape(metadata["assetId"])}</code><p class="note">Linked to the observed source object without inventing a revision.</p></div>
      <div class="arrow">→</div>
      <div class="node"><strong>KFP root run</strong><span class="tag">Orchestration</span><br><code>{html.escape(metadata["rootRunId"][:8])}…</code><p class="note">Owns lifecycle, pipeline identity, and task context.</p></div>
      <div class="arrow">→</div>
      <div class="node"><strong>Feature preparation</strong><span class="tag">Spark</span><br><code>features/customer-risk</code><p class="note">Reports the transformation and its output.</p></div>
      <div class="arrow">→</div>
      <div class="node"><strong>Candidate model</strong><span class="tag">MLflow</span><br><code>{html.escape(metadata["model"])}</code><p class="note">Links model metadata back to the producing run.</p></div>
    </div>
  </section>
  <section>
    <h2>Audit timeline</h2>
    <table><thead><tr><th>Time</th><th>State</th><th>Job</th><th>Run</th><th>Data edges</th></tr></thead><tbody>{rows}</tbody></table>
  </section>
  <section>
    <h2>Model decision</h2>
    <p><span class="tag">CANDIDATE</span> Holdout accuracy <strong>{html.escape(str(metadata["metrics"]["holdoutAccuracy"]))}</strong> across <strong>{html.escape(str(metadata["metrics"]["trainingRows"]))}</strong> training rows. The metric is reported by the training workload and linked to the candidate model.</p>
  </section>
  <section>
    <h2>What this proves—and what it does not</h2>
    <ul>{claims}</ul>
    <p class="note">Marquez backend: <a href="{html.escape(metadata["marquezUrl"])}">{html.escape(metadata["marquezUrl"])}</a>. Raw events and context documents are included beside this report for inspection and replay.</p>
  </section>
</main></body></html>"""


def write_showcase(
    bundle: ShowcaseBundle,
    output_dir: Path,
    contracts_dir: Path,
    *,
    emit: bool = False,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Validate and write the showcase artefact pack, optionally emitting to Marquez."""

    _validate_bundle(bundle, contracts_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "contexts.json").write_text(
        json.dumps(list(bundle.contexts), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "events.json").write_text(
        json.dumps([_json_event(event) for event in bundle.events], indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "outbox.json").write_text(
        json.dumps(list(bundle.outbox), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "showcase.json").write_text(
        json.dumps(bundle.metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output_dir / "audit-report.html").write_text(_render_html(bundle), encoding="utf-8")

    emitted = 0
    if emit:
        emitter = LineageEmitter(settings or Settings())
        try:
            for event in bundle.events:
                emitter.emit(event)
                emitted += 1
        finally:
            emitter.close()

    result = {
        "outputDir": str(output_dir),
        "auditReport": str(output_dir / "audit-report.html"),
        "events": str(output_dir / "events.json"),
        "contexts": str(output_dir / "contexts.json"),
        "outbox": str(output_dir / "outbox.json"),
        "eventCount": len(bundle.events),
        "emittedToMarquez": emitted,
        "contractProfile": bundle.metadata["contractProfile"],
    }
    return result
