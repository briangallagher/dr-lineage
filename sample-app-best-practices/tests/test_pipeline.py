from __future__ import annotations

import yaml
from kfp import compiler

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
