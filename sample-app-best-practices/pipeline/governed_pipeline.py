"""KFP adapter for the local governed Parquet-to-model Slice 2 path."""

from kfp import dsl, kubernetes

CONFIG_ENV = {
    "PROJECT_NAMESPACE": "PROJECT_NAMESPACE",
    "CLUSTER_NAME": "CLUSTER_NAME",
    "MARQUEZ_URL": "MARQUEZ_URL",
    "DATA_REGISTRY_URL": "DATA_REGISTRY_URL",
    "MLFLOW_TRACKING_URI": "MLFLOW_TRACKING_URI",
    "MLFLOW_TRACKING_AUTH": "MLFLOW_TRACKING_AUTH",
    "MLFLOW_TRACKING_SERVER_CERT_PATH": "MLFLOW_TRACKING_SERVER_CERT_PATH",
}
SOURCE_ENV = {
    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
    "AWS_S3_BUCKET": "SOURCE_BUCKET",
    "AWS_S3_ENDPOINT": "S3_ENDPOINT",
    "AWS_DEFAULT_REGION": "S3_REGION",
}


def configure_governed_task(task, *, source_secret_name=None):
    kubernetes.use_config_map_as_env(task, "lineage-governed-config", CONFIG_ENV)
    kubernetes.use_field_path_as_env(
        task, env_name="KFP_RUN_ID", field_path="metadata.labels['pipeline/runid']"
    )
    kubernetes.use_field_path_as_env(task, env_name="KFP_POD_NAME", field_path="metadata.name")
    if source_secret_name is not None:
        kubernetes.use_secret_as_env(task, source_secret_name, SOURCE_ENV)
        kubernetes.use_config_map_as_volume(
            task, "data-registry-service-ca", "/var/run/data-registry-ca"
        )
    return task


def build_governed_pipeline(app_image: str):
    @dsl.container_component
    def root_start(root_run_id: dsl.OutputPath(str)):
        return dsl.ContainerSpec(
            image=app_image,
            command=["lineage-demo"],
            args=[
                "root-start",
                "--job-name",
                "governed-asset-to-model",
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
            final_status.error_code or "KFP run failed",
            job_name="governed-asset-to-model",
        )

    @dsl.container_component
    def train_component(
        project: str,
        collection: str,
        asset_name: str,
        expected_asset_uuid: str,
        source_secret_name: str,
        target_column: str,
        result: dsl.OutputPath(str),
    ):
        return dsl.ContainerSpec(
            image=app_image,
            command=["lineage-demo"],
            args=[
                "train-governed",
                "--project",
                project,
                "--collection",
                collection,
                "--asset-name",
                asset_name,
                "--expected-asset-uuid",
                expected_asset_uuid,
                "--source-secret-name",
                source_secret_name,
                "--target-column",
                target_column,
                "--result-path",
                result,
            ],
        )

    @dsl.pipeline(
        name="governed-asset-to-model",
        description="Resolve an authorized table, train a baseline, and log MLflow evidence.",
    )
    def pipeline(
        project: str = "scenario-b",
        collection: str = "forms",
        asset_name: str = "iso_form_extractions",
        expected_asset_uuid: str = "33fbc314-ea15-4805-a958-95b8d29dd67d",
        source_secret_name: str = "dataconnection-minio-iso-forms",
        target_column: str = "line_of_business",
    ):
        started = configure_governed_task(root_start().set_caching_options(False))
        exit_task = configure_governed_task(root_end().set_caching_options(False))
        with dsl.ExitHandler(exit_task=exit_task):
            configure_governed_task(
                train_component(
                    project=project,
                    collection=collection,
                    asset_name=asset_name,
                    expected_asset_uuid=expected_asset_uuid,
                    source_secret_name=source_secret_name,
                    target_column=target_column,
                )
                .after(started)
                .set_caching_options(False),
                source_secret_name=source_secret_name,
            )

    return pipeline
