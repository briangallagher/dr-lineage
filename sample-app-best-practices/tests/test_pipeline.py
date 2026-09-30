from __future__ import annotations

import yaml
from kfp import compiler

from pipeline.governed_pipeline import build_governed_pipeline
from pipeline.pipeline import build_pipeline


def test_pipeline_compiles_with_retries_caching_disabled_and_runtime_id(tmp_path) -> None:
    destination = tmp_path / "pipeline.yaml"
    compiler.Compiler().compile(
        pipeline_func=build_pipeline("registry/app:sha", "registry/spark:sha"),
        package_path=str(destination),
    )
    text = destination.read_text()
    document = next(yaml.safe_load_all(text))
    assert "{{$.pipeline_job_uuid}}" not in text
    assert "KFP_RUN_ID" in text
    assert "metadata.labels['pipeline/runid']" in text
    assert "python3 -m pip install" not in text
    assert "registry/spark:sha" in text
    assert "retryPolicy" in text
    # KFP 2.16 serializes disabled caching as an empty CachingOptions message.
    assert text.count("cachingOptions: {}") >= 5
    assert "enableCache: true" not in text
    assert document["pipelineInfo"]["name"] == "openlineage-data-registry-best-practices"


def test_governed_pipeline_compiles_with_project_credentials_and_kfp_context(tmp_path) -> None:
    destination = tmp_path / "governed.yaml"
    compiler.Compiler().compile(
        pipeline_func=build_governed_pipeline("registry/app:sha"),
        package_path=str(destination),
    )
    text = destination.read_text()
    document = next(yaml.safe_load_all(text))
    assert document["pipelineInfo"]["name"] == "governed-asset-to-model"
    assert "metadata.labels['pipeline/runid']" in text
    assert "metadata.name" in text
    assert "data-registry-service-ca" in text
    assert "lineage-governed-config" in text
    assert "MLFLOW_TRACKING_AUTH" in text
    assert "MLFLOW_TRACKING_SERVER_CERT_PATH" in text
    assert "dataconnection-minio-iso-forms" in text
    assert "AWS_S3_ENDPOINT" in text
    assert "AWS_S3_BUCKET" in text
    assert "envVar: SOURCE_BUCKET\n              secretKey: AWS_S3_BUCKET" in text
    assert "envVar: S3_ENDPOINT\n              secretKey: AWS_S3_ENDPOINT" in text
    assert "{{$.pipeline_job_uuid}}" not in text
    assert "enableCache: true" not in text
