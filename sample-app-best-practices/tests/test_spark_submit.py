from __future__ import annotations

from lineage_demo.config import Settings
from lineage_demo.spark_submit import build_spark_application


def test_spark_application_uses_native_listener_and_exact_parent() -> None:
    settings = Settings(cluster_name="cluster", project_namespace="project")
    manifest, result = build_spark_application(
        settings=settings,
        asset_id="asset",
        staged_uri="s3://sample-data/staging/asset/ingest/documents.csv",
        pipeline_job_id="3a6e0886-40b3-4cee-bddd-c983fd693155",
        spark_image="registry/spark:sha",
        fail=False,
        run_id="01994c99-64b7-72ac-a9c3-c35330905e77",
    )
    spec = manifest["spec"]
    conf = spec["sparkConf"]
    assert spec["restartPolicy"] == {"type": "Never"}
    assert spec["driver"]["serviceAccount"] == "pipeline-runner-dspa"
    assert spec["executor"]["serviceAccount"] == "pipeline-runner-dspa"
    assert "env" not in spec["driver"]
    assert "envFrom" not in spec["driver"]
    assert spec["driver"]["envVars"] == {"S3_ENDPOINT": settings.s3_endpoint}
    assert spec["driver"]["envSecretKeyRefs"]["AWS_ACCESS_KEY_ID"] == {
        "name": "object-store-credentials",
        "key": "AWS_ACCESS_KEY_ID",
    }
    assert spec["executor"]["envVars"] == spec["driver"]["envVars"]
    assert spec["executor"]["envSecretKeyRefs"] == spec["driver"]["envSecretKeyRefs"]
    assert conf["spark.extraListeners"].endswith("OpenLineageSparkListener")
    assert conf["spark.openlineage.applicationRunId"] == result.run_id
    assert conf["spark.openlineage.parentRunId"] == "3a6e0886-40b3-4cee-bddd-c983fd693155"
    assert conf["spark.openlineage.rootParentRunId"] == conf["spark.openlineage.parentRunId"]
    assert result.output_uri == (
        "s3://sample-data/transformed/asset/01994c99-64b7-72ac-a9c3-c35330905e77"
    )
