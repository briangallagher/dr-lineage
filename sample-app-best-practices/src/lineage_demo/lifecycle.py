"""KFP root OpenLineage lifecycle handling."""

from __future__ import annotations

from openlineage.client.event_v2 import RunState

from lineage_demo import facets
from lineage_demo.config import Settings
from lineage_demo.events import LineageEmitter
from lineage_demo.identities import root_run_id
from lineage_demo.processing import ROOT_JOB_NAME


def root_job_facets() -> dict:
    return {
        "jobType": facets.job_type("KFP", "DAG"),
        "documentation": facets.job_documentation(
            "Orchestrates registry ingestion, Spark transformation, and mock embeddings."
        ),
        "ownership": facets.ownership("rhoai-data-platform"),
        "tags": facets.tags({"application": ROOT_JOB_NAME, "environment": "demo"}),
        "sourceCodeLocation": facets.source_code_location(
            facets.REPOSITORY,
            "sample-app-best-practices/pipeline/pipeline.py",
        ),
    }


def start_root(
    settings: Settings,
    pipeline_job_id: str,
    emitter: LineageEmitter | None = None,
) -> str:
    run_id = root_run_id(pipeline_job_id)
    (emitter or LineageEmitter(settings)).run_event(
        state=RunState.START,
        run_id=run_id,
        job_namespace=settings.kfp_namespace,
        job_name=ROOT_JOB_NAME,
        job_facets=root_job_facets(),
        run_facets={
            "processing_engine": facets.processing_engine("Kubeflow Pipelines", "2.16.1", "1.53.0")
        },
    )
    return run_id


def finish_root(
    settings: Settings,
    pipeline_job_id: str,
    final_state: str,
    error: str = "",
    emitter: LineageEmitter | None = None,
) -> str:
    state_mapping = {
        "SUCCEEDED": RunState.COMPLETE,
        "COMPLETE": RunState.COMPLETE,
        "FAILED": RunState.FAIL,
        "FAIL": RunState.FAIL,
        "CANCELLED": RunState.ABORT,
        "CANCELED": RunState.ABORT,
        "ABORTED": RunState.ABORT,
    }
    state = state_mapping.get(final_state.upper(), RunState.FAIL)
    run_facets = {
        "processing_engine": facets.processing_engine("Kubeflow Pipelines", "2.16.1", "1.53.0")
    }
    if state != RunState.COMPLETE:
        run_facets["errorMessage"] = facets.error_message(error or f"KFP state: {final_state}")
    run_id = root_run_id(pipeline_job_id)
    (emitter or LineageEmitter(settings)).run_event(
        state=state,
        run_id=run_id,
        job_namespace=settings.kfp_namespace,
        job_name=ROOT_JOB_NAME,
        job_facets=root_job_facets(),
        run_facets=run_facets,
    )
    return run_id
