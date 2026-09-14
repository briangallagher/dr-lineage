"""Command-line entry point used by containers and local operator scripts."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import httpx
import uvicorn

from lineage_demo.config import get_settings
from lineage_demo.events import LineageEmitter
from lineage_demo.identities import canonical_s3_dataset, root_run_id
from lineage_demo.lifecycle import finish_root, root_job_facets, start_root
from lineage_demo.processing import (
    EMBED_JOB_NAME,
    INGEST_JOB_NAME,
    ROOT_JOB_NAME,
    _job_facets,
    embed,
    ingest,
)
from lineage_demo.spark_submit import submit_and_wait
from lineage_demo.storage import put_bytes, s3_client

LOG = logging.getLogger(__name__)


def _write(path: str, value: str) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(value, encoding="utf-8")


def _register(subparsers: argparse._SubParsersAction) -> None:
    serve = subparsers.add_parser("registry-serve")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8080)

    root_start = subparsers.add_parser("root-start")
    root_start.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    root_start.add_argument("--root-run-id-path", required=True)

    root_end = subparsers.add_parser("root-end")
    root_end.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    root_end.add_argument("--state", required=True)
    root_end.add_argument("--error", default="")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("--asset-id", required=True)
    ingest_parser.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    ingest_parser.add_argument("--failure-mode", default="none")
    ingest_parser.add_argument("--staged-uri-path", required=True)
    ingest_parser.add_argument("--ingest-run-id-path", required=True)

    spark_parser = subparsers.add_parser("spark-submit")
    spark_parser.add_argument("--asset-id", required=True)
    spark_parser.add_argument("--staged-uri", required=True)
    spark_parser.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    spark_parser.add_argument("--spark-image", required=True)
    spark_parser.add_argument("--failure-mode", default="none")
    spark_parser.add_argument("--transformed-uri-path", required=True)
    spark_parser.add_argument("--spark-run-id-path", required=True)

    embed_parser = subparsers.add_parser("embed")
    embed_parser.add_argument("--asset-id", required=True)
    embed_parser.add_argument("--transformed-uri", required=True)
    embed_parser.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    embed_parser.add_argument("--failure-mode", default="none")
    embed_parser.add_argument("--embedding-uri-path", required=True)
    embed_parser.add_argument("--embedding-run-id-path", required=True)

    seed = subparsers.add_parser("source-put")
    seed.add_argument("--uri", required=True)
    seed.add_argument("--file", required=True)

    publish = subparsers.add_parser("publish-jobs")
    publish.add_argument("--spark-job-name", default="transform-documents")

    register = subparsers.add_parser("register-asset")
    register.add_argument("--name", default="Customer documents")
    register.add_argument("--description", default="Mutable CSV source used by the lineage demo")
    register.add_argument("--owner", default="data-science")
    register.add_argument("--location", required=True)
    register.add_argument("--idempotency-key", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lineage-demo")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _register(subparsers)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    settings = get_settings()

    if args.command == "registry-serve":
        uvicorn.run(
            "lineage_demo.registry_api:create_app",
            factory=True,
            host=args.host,
            port=args.port,
        )
    elif args.command == "root-start":
        run_id = start_root(settings, args.pipeline_job_id)
        _write(args.root_run_id_path, run_id)
    elif args.command == "root-end":
        finish_root(settings, args.pipeline_job_id, args.state, args.error)
    elif args.command == "ingest":
        staged_uri, run_id = ingest(
            settings=settings,
            asset_id=args.asset_id,
            root_id=root_run_id(args.pipeline_job_id),
            fail=args.failure_mode == "ingest",
        )
        _write(args.staged_uri_path, staged_uri)
        _write(args.ingest_run_id_path, run_id)
    elif args.command == "spark-submit":
        result = submit_and_wait(
            settings=settings,
            asset_id=args.asset_id,
            staged_uri=args.staged_uri,
            pipeline_job_id=args.pipeline_job_id,
            spark_image=args.spark_image,
            fail=args.failure_mode == "spark",
        )
        _write(args.transformed_uri_path, result.output_uri)
        _write(args.spark_run_id_path, result.run_id)
    elif args.command == "embed":
        output_uri, run_id = embed(
            settings=settings,
            asset_id=args.asset_id,
            transformed_uri=args.transformed_uri,
            root_id=root_run_id(args.pipeline_job_id),
            fail=args.failure_mode == "embed",
        )
        _write(args.embedding_uri_path, output_uri)
        _write(args.embedding_run_id_path, run_id)
    elif args.command == "source-put":
        canonical_s3_dataset(args.uri)
        put_bytes(s3_client(settings), args.uri, Path(args.file).read_bytes())
    elif args.command == "register-asset":
        response = httpx.post(
            f"{settings.registry_url.rstrip('/')}/v1/assets",
            headers={"Idempotency-Key": args.idempotency_key},
            json={
                "name": args.name,
                "description": args.description,
                "owner": args.owner,
                "location": args.location,
            },
            timeout=30,
        )
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"Registry returned {response.status_code}: {response.text}")
        print(json.dumps(response.json(), indent=2))
    elif args.command == "publish-jobs":
        emitter = LineageEmitter(settings)
        emitter.job_event(
            job_namespace=settings.kfp_namespace,
            job_name=ROOT_JOB_NAME,
            job_facets=root_job_facets(),
        )
        emitter.job_event(
            job_namespace=settings.ingest_namespace,
            job_name=INGEST_JOB_NAME,
            job_facets=_job_facets(
                integration="DCH_MOCK",
                job_type_name="JOB",
                description="Resolve, validate, and stage a registered source.",
                path="src/lineage_demo/processing.py",
            ),
        )
        emitter.job_event(
            job_namespace=settings.spark_namespace,
            job_name=args.spark_job_name,
            job_facets=_job_facets(
                integration="SPARK",
                job_type_name="JOB",
                description="Normalize document text and write Parquet.",
                path="spark/transform.py",
            ),
        )
        emitter.job_event(
            job_namespace=settings.kfp_namespace,
            job_name=EMBED_JOB_NAME,
            job_facets=_job_facets(
                integration="KFP",
                job_type_name="JOB",
                description="Create deterministic mock embeddings.",
                path="src/lineage_demo/processing.py",
            ),
        )
    else:  # pragma: no cover - argparse enforces the command list
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
