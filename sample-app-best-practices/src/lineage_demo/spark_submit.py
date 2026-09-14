"""Create and wait for a real SparkApplication custom resource."""

from __future__ import annotations

import time
from dataclasses import dataclass

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from lineage_demo.config import Settings
from lineage_demo.identities import (
    parse_s3_uri,
    root_run_id,
    s3_uri,
    transformed_identity,
    uuid7,
)
from lineage_demo.processing import ROOT_JOB_NAME

GROUP = "sparkoperator.k8s.io"
VERSION = "v1beta2"
PLURAL = "sparkapplications"


@dataclass(frozen=True)
class SparkResult:
    output_uri: str
    run_id: str
    application_name: str


def _load_kubernetes() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def build_spark_application(
    *,
    settings: Settings,
    asset_id: str,
    staged_uri: str,
    pipeline_job_id: str,
    spark_image: str,
    fail: bool,
    run_id: str,
) -> tuple[dict, SparkResult]:
    bucket, _ = parse_s3_uri(staged_uri)
    output = transformed_identity(bucket, asset_id, run_id)
    output_uri = s3_uri(output)
    application_name = f"transform-{run_id[:8]}"
    root_id = root_run_id(pipeline_job_id)
    args = ["--input-uri", staged_uri, "--output-uri", output_uri]
    if fail:
        args.append("--fail")

    spark_conf = {
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": settings.marquez_url,
        "spark.openlineage.namespace": settings.spark_namespace,
        "spark.openlineage.appName": "transform-documents",
        "spark.openlineage.applicationRunId": run_id,
        "spark.openlineage.parentJobNamespace": settings.kfp_namespace,
        "spark.openlineage.parentJobName": ROOT_JOB_NAME,
        "spark.openlineage.parentRunId": root_id,
        "spark.openlineage.rootParentJobNamespace": settings.kfp_namespace,
        "spark.openlineage.rootParentJobName": ROOT_JOB_NAME,
        "spark.openlineage.rootParentRunId": root_id,
        "spark.openlineage.jobName.appendDatasetName": "false",
        "spark.openlineage.facets.columnLineage.disabled": "false",
        "spark.sql.parquet.compression.codec": "snappy",
    }
    pod = {
        "serviceAccount": "pipeline-runner-dspa",
        "cores": 1,
        "memory": "1g",
        # The RHOAI Spark Operator 2.4.0 CRD accepts the newer `env` and
        # `envFrom` fields but its submission path does not propagate them to
        # generated Spark pods. These legacy fields are deprecated upstream,
        # but are the secret-safe fields implemented by this operator release.
        "envSecretKeyRefs": {
            "AWS_ACCESS_KEY_ID": {
                "name": "object-store-credentials",
                "key": "AWS_ACCESS_KEY_ID",
            },
            "AWS_SECRET_ACCESS_KEY": {
                "name": "object-store-credentials",
                "key": "AWS_SECRET_ACCESS_KEY",
            },
            "AWS_DEFAULT_REGION": {
                "name": "object-store-credentials",
                "key": "AWS_DEFAULT_REGION",
            },
        },
        "envVars": {"S3_ENDPOINT": settings.s3_endpoint},
        "labels": {"app.kubernetes.io/part-of": "openlineage-best-practices"},
    }
    manifest = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "SparkApplication",
        "metadata": {
            "name": application_name,
            "namespace": settings.project_namespace,
            "labels": {
                "app.kubernetes.io/name": "spark-transform",
                "app.kubernetes.io/part-of": "openlineage-best-practices",
                "lineage.rhoai.io/root-run-id": root_id,
            },
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "sparkVersion": "4.0.1",
            "image": spark_image,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/work-dir/transform.py",
            "arguments": args,
            "sparkConf": spark_conf,
            "restartPolicy": {"type": "Never"},
            "driver": pod,
            "executor": {**pod, "instances": 1},
        },
    }
    return manifest, SparkResult(output_uri, run_id, application_name)


def submit_and_wait(
    *,
    settings: Settings,
    asset_id: str,
    staged_uri: str,
    pipeline_job_id: str,
    spark_image: str,
    fail: bool = False,
    timeout_seconds: int = 1800,
) -> SparkResult:
    run_id = str(uuid7())
    manifest, result = build_spark_application(
        settings=settings,
        asset_id=asset_id,
        staged_uri=staged_uri,
        pipeline_job_id=pipeline_job_id,
        spark_image=spark_image,
        fail=fail,
        run_id=run_id,
    )
    _load_kubernetes()
    api = client.CustomObjectsApi()
    try:
        api.create_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=settings.project_namespace,
            plural=PLURAL,
            body=manifest,
        )
    except ApiException as exc:
        raise RuntimeError(
            f"Could not create SparkApplication {result.application_name}: {exc}"
        ) from exc

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        resource = api.get_namespaced_custom_object_status(
            group=GROUP,
            version=VERSION,
            namespace=settings.project_namespace,
            plural=PLURAL,
            name=result.application_name,
        )
        state = resource.get("status", {}).get("applicationState", {}).get("state", "")
        if state == "COMPLETED":
            return result
        if state in {"FAILED", "FAILING", "UNKNOWN"}:
            error = resource.get("status", {}).get("errorMessage", "No error message reported")
            raise RuntimeError(
                f"SparkApplication {result.application_name} entered {state}: {error}"
            )
        time.sleep(5)
    raise TimeoutError(f"SparkApplication {result.application_name} did not finish in time")
