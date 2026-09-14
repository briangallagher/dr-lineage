from __future__ import annotations

from openlineage.client.generated import (
    column_lineage_dataset,
    dataset_type_dataset,
    datasource_dataset,
    documentation_dataset,
    documentation_job,
    error_message_run,
    execution_parameters_run,
    job_type_job,
    lifecycle_state_change_dataset,
    output_statistics_output_dataset,
    ownership_dataset,
    ownership_job,
    parent_run,
    processing_engine_run,
    schema_dataset,
    source_code_location_job,
    storage_dataset,
    symlinks_dataset,
    tags_job,
)

from lineage_demo import facets


def test_builders_reference_the_schemas_generated_for_openlineage_1_53() -> None:
    checks = [
        (facets.documentation("x"), documentation_dataset.DocumentationDatasetFacet),
        (facets.job_documentation("x"), documentation_job.DocumentationJobFacet),
        (facets.data_source("x", "s3://x"), datasource_dataset.DatasourceDatasetFacet),
        (facets.storage("S3", "CSV"), storage_dataset.StorageDatasetFacet),
        (facets.dataset_type("FILE"), dataset_type_dataset.DatasetTypeDatasetFacet),
        (facets.schema([]), schema_dataset.SchemaDatasetFacet),
        (
            facets.lifecycle_change("CREATE"),
            lifecycle_state_change_dataset.LifecycleStateChangeDatasetFacet,
        ),
        (
            facets.parent_run(
                parent_namespace="kfp://x",
                parent_name="pipeline",
                parent_run_id="3a6e0886-40b3-4cee-bddd-c983fd693155",
            ),
            parent_run.ParentRunFacet,
        ),
        (
            facets.execution_parameters({"a": 1}),
            execution_parameters_run.ExecutionParametersRunFacet,
        ),
        (
            facets.processing_engine("Python", "3.11", "1.53.0"),
            processing_engine_run.ProcessingEngineRunFacet,
        ),
        (facets.error_message("bad"), error_message_run.ErrorMessageRunFacet),
        (
            facets.output_statistics(row_count=1, size=2, file_count=1),
            output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet,
        ),
        (
            facets.column_lineage({"out": ["in"]}, {"namespace": "s3://x", "name": "input"}),
            column_lineage_dataset.ColumnLineageDatasetFacet,
        ),
        (facets.job_type("KFP", "DAG"), job_type_job.JobTypeJobFacet),
        (facets.ownership("team"), ownership_job.OwnershipJobFacet),
        (facets.dataset_ownership("team"), ownership_dataset.OwnershipDatasetFacet),
        (facets.tags({"a": "b"}), tags_job.TagsJobFacet),
        (
            facets.source_code_location("https://example.test/repo", "src/app.py"),
            source_code_location_job.SourceCodeLocationJobFacet,
        ),
        (
            facets.symlinks("s3://x", "input", "OBJECT"),
            symlinks_dataset.SymlinksDatasetFacet,
        ),
    ]
    for value, generated_type in checks:
        assert value["_schemaURL"] == generated_type._get_schema()
