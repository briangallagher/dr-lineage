"""Ingestion and deterministic mock-embedding processing jobs."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import tempfile
from pathlib import Path

import pyarrow.parquet as parquet
from openlineage.client.event_v2 import RunState

from lineage_demo import facets
from lineage_demo.config import Settings
from lineage_demo.events import LineageEmitter, input_dataset, output_dataset
from lineage_demo.identities import (
    DatasetIdentity,
    canonical_s3_dataset,
    embeddings_identity,
    parse_s3_uri,
    s3_uri,
    staged_identity,
    uuid7,
)
from lineage_demo.registry_client import get_asset
from lineage_demo.storage import (
    download_object,
    list_objects,
    put_create_only,
    read_bytes,
    s3_client,
)

LOG = logging.getLogger(__name__)
ROOT_JOB_NAME = "sample-app-best-practices"
INGEST_JOB_NAME = "read-and-stage"
EMBED_JOB_NAME = "create-mock-embeddings"
SOURCE_FIELDS = [
    {"name": "title", "type": "STRING"},
    {"name": "category", "type": "STRING"},
    {"name": "text", "type": "STRING"},
]


def _dataset_facets(
    identity: DatasetIdentity,
    file_format: str,
    fields: list[dict[str, str]],
) -> dict:
    return {
        "schema": facets.schema(fields),
        "dataSource": facets.data_source(identity.namespace, identity.namespace),
        "storage": facets.storage("S3", file_format),
        "datasetType": facets.dataset_type("FILE"),
    }


def _job_facets(*, integration: str, job_type_name: str, description: str, path: str) -> dict:
    return {
        "jobType": facets.job_type(integration, job_type_name),
        "documentation": facets.job_documentation(description),
        "ownership": facets.ownership("rhoai-data-platform"),
        "tags": facets.tags({"application": "sample-app-best-practices", "environment": "demo"}),
        "sourceCodeLocation": facets.source_code_location(
            facets.REPOSITORY, f"sample-app-best-practices/{path}"
        ),
    }


def _parent_facets(settings: Settings, root_id: str, parameters: dict) -> dict:
    return {
        "parent": facets.parent_run(
            parent_namespace=settings.kfp_namespace,
            parent_name=ROOT_JOB_NAME,
            parent_run_id=root_id,
        ),
        "executionParameters": facets.execution_parameters(parameters),
        "processing_engine": facets.processing_engine("Python", "3.11", "1.53.0"),
    }


def validate_source_csv(payload: bytes) -> int:
    text = payload.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames != ["title", "category", "text"]:
        raise ValueError(
            "Expected CSV columns title,category,text; no durable row identifier is permitted"
        )
    count = 0
    for row in reader:
        if not all(row.get(field, "").strip() for field in reader.fieldnames):
            raise ValueError(f"Row {count + 2} contains an empty required value")
        count += 1
    if count == 0:
        raise ValueError("Source CSV is empty")
    return count


def ingest(
    *,
    settings: Settings,
    asset_id: str,
    root_id: str,
    fail: bool = False,
    emitter: LineageEmitter | None = None,
) -> tuple[str, str]:
    emitter = emitter or LineageEmitter(settings)
    run_id = str(uuid7())
    asset = get_asset(settings.registry_url, asset_id)
    raw = canonical_s3_dataset(asset.location)
    bucket, _ = parse_s3_uri(asset.location)
    staged = staged_identity(bucket, asset_id, run_id)
    raw_dataset = input_dataset(raw, _dataset_facets(raw, "CSV", SOURCE_FIELDS))
    run_facets = _parent_facets(settings, root_id, {"assetId": asset_id, "mock": False})
    job_facets = _job_facets(
        integration="DCH_MOCK",
        job_type_name="JOB",
        description="Resolve a Data Registry asset, validate its physical object, and stage it.",
        path="src/lineage_demo/processing.py",
    )

    emitter.run_event(
        state=RunState.START,
        run_id=run_id,
        job_namespace=settings.ingest_namespace,
        job_name=INGEST_JOB_NAME,
        run_facets=run_facets,
        job_facets=job_facets,
        inputs=[raw_dataset],
    )
    try:
        if fail:
            raise RuntimeError("Intentional ingestion failure requested by failure_mode")
        client = s3_client(settings)
        payload = read_bytes(client, asset.location)
        row_count = validate_source_csv(payload)
        put_create_only(client, staged, payload)
        output = output_dataset(
            staged,
            _dataset_facets(staged, "CSV", SOURCE_FIELDS),
            {
                "outputStatistics": facets.output_statistics(
                    row_count=row_count, size=len(payload), file_count=1
                )
            },
        )
        emitter.run_event(
            state=RunState.COMPLETE,
            run_id=run_id,
            job_namespace=settings.ingest_namespace,
            job_name=INGEST_JOB_NAME,
            run_facets=run_facets,
            job_facets=job_facets,
            inputs=[raw_dataset],
            outputs=[output],
        )
        LOG.info("Staged %s rows at %s", row_count, s3_uri(staged))
        return s3_uri(staged), run_id
    except BaseException as exc:
        terminal = (
            RunState.ABORT if isinstance(exc, (KeyboardInterrupt, SystemExit)) else RunState.FAIL
        )
        emitter.run_event(
            state=terminal,
            run_id=run_id,
            job_namespace=settings.ingest_namespace,
            job_name=INGEST_JOB_NAME,
            run_facets={**run_facets, "errorMessage": facets.error_message(str(exc))},
            job_facets=job_facets,
            inputs=[raw_dataset],
        )
        raise


def _mock_vector(value: str, dimensions: int = 8) -> list[float]:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return [round((digest[index] / 127.5) - 1.0, 6) for index in range(dimensions)]


def embed(
    *,
    settings: Settings,
    asset_id: str,
    transformed_uri: str,
    root_id: str,
    fail: bool = False,
    emitter: LineageEmitter | None = None,
) -> tuple[str, str]:
    emitter = emitter or LineageEmitter(settings)
    run_id = str(uuid7())
    input_identity = canonical_s3_dataset(transformed_uri)
    bucket, _ = parse_s3_uri(transformed_uri)
    output_identity = embeddings_identity(bucket, asset_id, run_id)
    transformed_fields = [
        {"name": "title", "type": "STRING"},
        {"name": "category", "type": "STRING"},
        {"name": "normalized_text", "type": "STRING"},
        {"name": "word_count", "type": "LONG"},
    ]
    output_fields = [
        {"name": "record_index", "type": "LONG", "description": "Run-local position; not durable"},
        {"name": "title", "type": "STRING"},
        {"name": "embedding", "type": "ARRAY<FLOAT>"},
    ]
    transformed_dataset = input_dataset(
        input_identity, _dataset_facets(input_identity, "PARQUET", transformed_fields)
    )
    run_facets = _parent_facets(
        settings,
        root_id,
        {"assetId": asset_id, "algorithm": "sha256-demo", "dimensions": 8, "mock": True},
    )
    job_facets = _job_facets(
        integration="KFP",
        job_type_name="JOB",
        description="Create deterministic mock embeddings from Spark output.",
        path="src/lineage_demo/processing.py",
    )
    emitter.run_event(
        state=RunState.START,
        run_id=run_id,
        job_namespace=settings.kfp_namespace,
        job_name=EMBED_JOB_NAME,
        run_facets=run_facets,
        job_facets=job_facets,
        inputs=[transformed_dataset],
    )
    try:
        if fail:
            raise RuntimeError("Intentional embedding failure requested by failure_mode")
        client = s3_client(settings)
        rows: list[dict] = []
        with tempfile.TemporaryDirectory(prefix="lineage-embedding-") as temporary:
            root = Path(temporary)
            for index, item in enumerate(list_objects(client, transformed_uri, suffix=".parquet")):
                local = root / f"part-{index}.parquet"
                download_object(
                    client,
                    bucket=item["Bucket"],
                    key=item["Key"],
                    destination=local,
                )
                table = parquet.read_table(local, columns=["title", "normalized_text"])
                for title, text in zip(
                    table.column("title").to_pylist(),
                    table.column("normalized_text").to_pylist(),
                    strict=True,
                ):
                    rows.append(
                        {
                            "record_index": len(rows),
                            "title": title,
                            "embedding": _mock_vector(text),
                        }
                    )
        if not rows:
            raise ValueError("Spark output contained no Parquet rows")
        payload = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows).encode()
        put_create_only(client, output_identity, payload)
        output_facets = _dataset_facets(output_identity, "JSONL", output_fields)
        output_facets["columnLineage"] = facets.column_lineage(
            {"title": ["title"], "embedding": ["normalized_text"]},
            input_identity.as_dict(),
        )
        embedding_dataset = output_dataset(
            output_identity,
            output_facets,
            {
                "outputStatistics": facets.output_statistics(
                    row_count=len(rows), size=len(payload), file_count=1
                )
            },
        )
        emitter.run_event(
            state=RunState.COMPLETE,
            run_id=run_id,
            job_namespace=settings.kfp_namespace,
            job_name=EMBED_JOB_NAME,
            run_facets=run_facets,
            job_facets=job_facets,
            inputs=[transformed_dataset],
            outputs=[embedding_dataset],
        )
        LOG.info("Wrote %s mock embeddings to %s", len(rows), s3_uri(output_identity))
        return s3_uri(output_identity), run_id
    except BaseException as exc:
        terminal = (
            RunState.ABORT if isinstance(exc, (KeyboardInterrupt, SystemExit)) else RunState.FAIL
        )
        emitter.run_event(
            state=terminal,
            run_id=run_id,
            job_namespace=settings.kfp_namespace,
            job_name=EMBED_JOB_NAME,
            run_facets={**run_facets, "errorMessage": facets.error_message(str(exc))},
            job_facets=job_facets,
            inputs=[transformed_dataset],
        )
        raise
