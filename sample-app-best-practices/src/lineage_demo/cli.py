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
from lineage_demo.evidence_import import import_live_evidence, write_evidence
from lineage_demo.identities import canonical_s3_dataset, root_run_id
from lineage_demo.lifecycle import finish_root, root_job_facets, start_root
from lineage_demo.native_validation import (
    build_native_path_validation,
    write_native_path_validation,
)
from lineage_demo.processing import (
    EMBED_JOB_NAME,
    INGEST_JOB_NAME,
    ROOT_JOB_NAME,
    _job_facets,
    embed,
    ingest,
)
from lineage_demo.product_demo import build_product_demo, write_product_demo
from lineage_demo.replay_api import create_app
from lineage_demo.showcase import SHOWCASE_ROOT_RUN_ID, build_showcase, write_showcase
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
    root_start.add_argument("--job-name", default=ROOT_JOB_NAME)

    root_end = subparsers.add_parser("root-end")
    root_end.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    root_end.add_argument("--state", required=True)
    root_end.add_argument("--error", default="")
    root_end.add_argument("--job-name", default=ROOT_JOB_NAME)

    governed = subparsers.add_parser("train-governed")
    governed.add_argument("--project", required=True)
    governed.add_argument("--collection", required=True)
    governed.add_argument("--asset-name", required=True)
    governed.add_argument("--expected-asset-uuid", required=True)
    governed.add_argument("--source-secret-name", required=True)
    governed.add_argument("--source-bucket", default=os.environ.get("SOURCE_BUCKET", ""))
    governed.add_argument("--target-column", required=True)
    governed.add_argument("--registry-url", default=os.environ.get("DATA_REGISTRY_URL", ""))
    governed.add_argument(
        "--registry-ca-file",
        default=os.environ.get("DATA_REGISTRY_CA", "/var/run/data-registry-ca/service-ca.crt"),
    )
    governed.add_argument(
        "--token-file", default="/var/run/secrets/kubernetes.io/serviceaccount/token"
    )
    governed.add_argument("--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", ""))
    governed.add_argument("--pipeline-job-id", default=os.environ.get("KFP_RUN_ID"))
    governed.add_argument("--pod-name", default=os.environ.get("KFP_POD_NAME"))
    governed.add_argument("--result-path", required=True)

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

    showcase = subparsers.add_parser(
        "showcase", help="Build the local product-level KFP lineage showcase"
    )
    showcase.add_argument("--output-dir", default="build/product-showcase")
    showcase.add_argument("--run-id", default=SHOWCASE_ROOT_RUN_ID)
    showcase.add_argument(
        "--emit",
        action="store_true",
        help="Send the generated OpenLineage events to the configured Marquez endpoint",
    )

    product_demo = subparsers.add_parser(
        "product-demo", help="Build the replayable stakeholder-facing lineage cockpit"
    )
    product_demo.add_argument("--output-dir", default="build/product-demo")
    product_demo.add_argument("--emit", action="store_true")
    product_demo.add_argument("--verified-evidence", default="")

    poc = subparsers.add_parser(
        "poc", help="Build the canonical offline PM and architecture lineage POC"
    )
    poc.add_argument("--output-dir", default="build/poc")
    poc.add_argument(
        "--verified-evidence",
        default="",
        help="Use a sanitized read-only RHOAI evidence snapshot instead of the bundled one",
    )

    native_validation = subparsers.add_parser(
        "native-validation", help="Run the local-only native KFP path checkpoint"
    )
    native_validation.add_argument("--output-dir", default="build/native-validation")

    evidence = subparsers.add_parser(
        "import-rhoai-evidence",
        help="Import one live KFP run through local read-only port-forwards",
    )
    evidence.add_argument("--kfp-url", required=True)
    evidence.add_argument("--run-id", required=True)
    evidence.add_argument(
        "--deployment", default=os.environ.get("RHOAI_LINEAGE_DEPLOYMENT", "")
    )
    evidence.add_argument("--marquez-url", default="")
    evidence.add_argument("--registry-url", default="")
    evidence.add_argument(
        "--registry-token", default=os.environ.get("RHOAI_DATA_REGISTRY_TOKEN", "")
    )
    evidence.add_argument("--mlflow-url", default="")
    evidence.add_argument("--mlflow-token", default=os.environ.get("RHOAI_MLFLOW_TOKEN", ""))
    evidence.add_argument("--mlflow-workspace", default="")
    evidence.add_argument("--output", default="build/product-demo/live-rhoai-evidence.json")

    demo_serve = subparsers.add_parser(
        "demo-serve", help="Serve the local product demo and replay API"
    )
    demo_serve.add_argument("--data-dir", default="build/product-demo")
    demo_serve.add_argument("--host", default="127.0.0.1")
    demo_serve.add_argument("--port", type=int, default=8090)


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
        run_id = start_root(settings, args.pipeline_job_id, job_name=args.job_name)
        _write(args.root_run_id_path, run_id)
    elif args.command == "root-end":
        finish_root(settings, args.pipeline_job_id, args.state, args.error, job_name=args.job_name)
    elif args.command == "train-governed":
        from lineage_demo.governed_model import AssetReference, run_governed_training

        if not args.registry_url or not args.tracking_uri:
            parser.error("DATA_REGISTRY_URL and MLFLOW_TRACKING_URI are required")
        result = run_governed_training(
            settings=settings,
            registry_url=args.registry_url,
            registry_ca_file=args.registry_ca_file,
            token_file=args.token_file,
            reference=AssetReference(
                args.project, args.collection, args.asset_name, args.expected_asset_uuid
            ),
            source_secret_name=args.source_secret_name,
            source_bucket=args.source_bucket,
            target_column=args.target_column,
            tracking_uri=args.tracking_uri,
            pipeline_job_id=args.pipeline_job_id,
            pod_name=args.pod_name,
        )
        _write(args.result_path, json.dumps(result, sort_keys=True))
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
    elif args.command == "showcase":
        fixture_root = Path(__file__).resolve().parents[2]
        bundle = build_showcase(settings, root_run_id=args.run_id)
        result = write_showcase(
            bundle,
            Path(args.output_dir),
            fixture_root / "contracts",
            emit=args.emit,
            settings=settings,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "product-demo":
        fixture_root = Path(__file__).resolve().parents[2]
        evidence = None
        if args.verified_evidence:
            evidence = json.loads(Path(args.verified_evidence).read_text(encoding="utf-8"))
        document = build_product_demo(settings, fixture_root, verified_evidence=evidence)
        result = write_product_demo(
            document,
            Path(args.output_dir),
            fixture_root,
            emit=args.emit,
            settings=settings,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "poc":
        fixture_root = Path(__file__).resolve().parents[2]
        evidence = None
        if args.verified_evidence:
            evidence = json.loads(Path(args.verified_evidence).read_text(encoding="utf-8"))
        document = build_product_demo(settings, fixture_root, verified_evidence=evidence)
        result = write_product_demo(
            document,
            Path(args.output_dir),
            fixture_root,
            emit=False,
            settings=settings,
        )
        result["mode"] = "offline-deterministic-poc"
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "native-validation":
        fixture_root = Path(__file__).resolve().parents[2]
        document = build_product_demo(settings, fixture_root)
        validation = build_native_path_validation(document)
        result = write_native_path_validation(validation, Path(args.output_dir))
        result.update(
            {
                "status": validation["status"],
                "recommendation": validation["recommendation"],
                "checkCount": len(validation["checks"]),
                "evidenceState": validation["evidenceState"],
            }
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "import-rhoai-evidence":
        evidence = import_live_evidence(
            kfp_url=args.kfp_url,
            run_id=args.run_id,
            deployment=args.deployment or None,
            marquez_url=args.marquez_url or None,
            registry_url=args.registry_url or None,
            registry_token=args.registry_token or None,
            mlflow_url=args.mlflow_url or None,
            mlflow_token=args.mlflow_token or None,
            mlflow_workspace=args.mlflow_workspace or None,
        )
        output = write_evidence(evidence, Path(args.output))
        print(
            json.dumps(
                {
                    "output": str(output),
                    "evidenceLevel": evidence["evidenceLevel"],
                    "runId": evidence["runId"],
                    "pipelineId": evidence["pipelineId"],
                    "state": evidence["state"],
                    "hasOpenLineage": "openLineage" in evidence,
                    "hasDataRegistry": "dataRegistry" in evidence,
                    "hasMLflow": "mlflow" in evidence,
                    "limitations": evidence["limitations"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    elif args.command == "demo-serve":
        uvicorn.run(create_app(args.data_dir), host=args.host, port=args.port)
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
