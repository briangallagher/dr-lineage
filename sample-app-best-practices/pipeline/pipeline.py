"""KFP v2 pipeline definition for the OpenLineage learning application."""

from kfp import dsl, kubernetes

CONFIG_ENV = {
    "PROJECT_NAMESPACE": "PROJECT_NAMESPACE",
    "CLUSTER_NAME": "CLUSTER_NAME",
    "REGISTRY_URL": "REGISTRY_URL",
    "MARQUEZ_URL": "MARQUEZ_URL",
    "S3_ENDPOINT": "S3_ENDPOINT",
    "S3_REGION": "S3_REGION",
}
SECRET_ENV = {
    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
}


def configure_task(task, *, storage: bool = False):
    kubernetes.use_config_map_as_env(task, "application-config", CONFIG_ENV)
    # RHOAI KFP 2.16 leaves PIPELINE_JOB_ID_PLACEHOLDER literal in custom
    # container arguments. The workflow controller does put the real KFP run
    # UUID in this pod label, so use the Downward API as the correlation source.
    kubernetes.use_field_path_as_env(
        task,
        env_name="KFP_RUN_ID",
        field_path="metadata.labels['pipeline/runid']",
    )
    if storage:
        kubernetes.use_secret_as_env(task, "object-store-credentials", SECRET_ENV)
    return task


def build_pipeline(app_image: str, spark_image: str):
    @dsl.container_component
    def root_start(root_run_id: dsl.OutputPath(str)):
        return dsl.ContainerSpec(
            image=app_image,
            command=["lineage-demo"],
            args=[
                "root-start",
                "--root-run-id-path",
                root_run_id,
            ],
        )

    @dsl.component(base_image=app_image, install_kfp_package=False)
    def root_end(final_status: dsl.PipelineTaskFinalStatus):
        import os

        from lineage_demo.config import get_settings
        from lineage_demo.lifecycle import finish_root

        finish_root(
            get_settings(),
            os.environ["KFP_RUN_ID"],
            final_status.state,
            final_status.error_message or final_status.error_code or "",
        )

    @dsl.container_component
    def ingest_component(
        asset_id: str,
        failure_mode: str,
        staged_uri: dsl.OutputPath(str),
        ingest_run_id: dsl.OutputPath(str),
    ):
        return dsl.ContainerSpec(
            image=app_image,
            command=["lineage-demo"],
            args=[
                "ingest",
                "--asset-id",
                asset_id,
                "--failure-mode",
                failure_mode,
                "--staged-uri-path",
                staged_uri,
                "--ingest-run-id-path",
                ingest_run_id,
            ],
        )

    @dsl.container_component
    def spark_component(
        asset_id: str,
        staged_uri: str,
        failure_mode: str,
        transformed_uri: dsl.OutputPath(str),
        spark_run_id: dsl.OutputPath(str),
    ):
        return dsl.ContainerSpec(
            image=app_image,
            command=["lineage-demo"],
            args=[
                "spark-submit",
                "--asset-id",
                asset_id,
                "--staged-uri",
                staged_uri,
                "--spark-image",
                spark_image,
                "--failure-mode",
                failure_mode,
                "--transformed-uri-path",
                transformed_uri,
                "--spark-run-id-path",
                spark_run_id,
            ],
        )

    @dsl.container_component
    def embedding_component(
        asset_id: str,
        transformed_uri: str,
        failure_mode: str,
        embedding_uri: dsl.OutputPath(str),
        embedding_run_id: dsl.OutputPath(str),
    ):
        return dsl.ContainerSpec(
            image=app_image,
            command=["lineage-demo"],
            args=[
                "embed",
                "--asset-id",
                asset_id,
                "--transformed-uri",
                transformed_uri,
                "--failure-mode",
                failure_mode,
                "--embedding-uri-path",
                embedding_uri,
                "--embedding-run-id-path",
                embedding_run_id,
            ],
        )

    @dsl.pipeline(
        name="OpenLineage Data Registry Best Practices",
        description="Registry to ingestion to Spark to mock embeddings with OpenLineage.",
    )
    def pipeline(asset_id: str, failure_mode: str = "none"):
        started = configure_task(root_start().set_caching_options(False))
        exit_task = configure_task(
            root_end().set_caching_options(False)
        )
        with dsl.ExitHandler(exit_task=exit_task):
            ingested = configure_task(
                ingest_component(asset_id=asset_id, failure_mode=failure_mode)
                .after(started)
                .set_caching_options(False)
                .set_retry(num_retries=1, backoff_duration="5s", backoff_factor=2.0),
                storage=True,
            )
            transformed = configure_task(
                spark_component(
                    asset_id=asset_id,
                    staged_uri=ingested.outputs["staged_uri"],
                    failure_mode=failure_mode,
                )
                .set_caching_options(False)
                .set_retry(num_retries=1, backoff_duration="10s", backoff_factor=2.0),
                storage=True,
            )
            configure_task(
                embedding_component(
                    asset_id=asset_id,
                    transformed_uri=transformed.outputs["transformed_uri"],
                    failure_mode=failure_mode,
                )
                .set_caching_options(False)
                .set_retry(num_retries=1, backoff_duration="5s", backoff_factor=2.0),
                storage=True,
            )

    return pipeline
