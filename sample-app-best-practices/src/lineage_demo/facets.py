"""Builders for the standard OpenLineage facets used by the sample."""

from __future__ import annotations

from typing import Any

REPOSITORY = "https://github.com/briangallagher/dr-lineage"
PRODUCER = f"{REPOSITORY}/tree/main/sample-app-best-practices#v0.1.0"
FACET_BASE = "https://openlineage.io/spec/facets"


def base(schema: str, version: str = "1-0-0") -> dict[str, str]:
    definition = schema.removesuffix(".json")
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"{FACET_BASE}/{version}/{schema}#/$defs/{definition}",
    }


def symlinks(namespace: str, name: str, identifier_type: str) -> dict[str, Any]:
    return {
        **base("SymlinksDatasetFacet.json", "1-0-1"),
        "identifiers": [{"namespace": namespace, "name": name, "type": identifier_type}],
    }


def documentation(description: str) -> dict[str, Any]:
    return {**base("DocumentationDatasetFacet.json", "1-1-0"), "description": description}


def job_documentation(description: str) -> dict[str, Any]:
    return {**base("DocumentationJobFacet.json", "1-1-0"), "description": description}


def job_type(integration: str, job_type_name: str) -> dict[str, Any]:
    return {
        **base("JobTypeJobFacet.json", "2-0-4"),
        "processingType": "BATCH",
        "integration": integration,
        "jobType": job_type_name,
        "emissionPattern": {
            "eventTrigger": "EVENT_BASED",
            "eventContentMode": "ACCUMULATIVE",
        },
    }


def ownership(owner: str) -> dict[str, Any]:
    return {
        **base("OwnershipJobFacet.json", "1-0-1"),
        "owners": [{"name": owner, "type": "MAINTAINER"}],
    }


def dataset_ownership(owner: str) -> dict[str, Any]:
    return {
        **base("OwnershipDatasetFacet.json", "1-0-1"),
        "owners": [{"name": owner, "type": "MAINTAINER"}],
    }


def tags(values: dict[str, str]) -> dict[str, Any]:
    return {
        **base("TagsJobFacet.json"),
        "tags": [{"key": key, "value": value} for key, value in sorted(values.items())],
    }


def source_code_location(url: str, repo_path: str) -> dict[str, Any]:
    return {
        **base("SourceCodeLocationJobFacet.json", "1-1-0"),
        "type": "git",
        "url": url,
        "path": repo_path,
    }


def data_source(name: str, uri: str) -> dict[str, Any]:
    return {**base("DatasourceDatasetFacet.json", "1-0-1"), "name": name, "uri": uri}


def storage(storage_layer: str = "S3", file_format: str | None = None) -> dict[str, Any]:
    facet: dict[str, Any] = {
        **base("StorageDatasetFacet.json", "1-0-1"),
        "storageLayer": storage_layer,
    }
    if file_format:
        facet["fileFormat"] = file_format
    return facet


def dataset_type(dataset_type_name: str = "FILE") -> dict[str, Any]:
    return {
        **base("DatasetTypeDatasetFacet.json", "1-0-1"),
        "datasetType": dataset_type_name,
    }


def schema(fields: list[dict[str, str]]) -> dict[str, Any]:
    return {**base("SchemaDatasetFacet.json", "1-2-0"), "fields": fields}


def lifecycle_change(change: str = "CREATE") -> dict[str, Any]:
    return {
        **base("LifecycleStateChangeDatasetFacet.json", "1-0-1"),
        "lifecycleStateChange": change,
    }


def parent_run(
    *,
    parent_namespace: str,
    parent_name: str,
    parent_run_id: str,
    root_namespace: str | None = None,
    root_name: str | None = None,
    root_run_id: str | None = None,
) -> dict[str, Any]:
    facet: dict[str, Any] = {
        **base("ParentRunFacet.json", "1-2-0"),
        "job": {"namespace": parent_namespace, "name": parent_name},
        "run": {"runId": parent_run_id},
    }
    if root_namespace and root_name and root_run_id:
        facet["root"] = {
            "job": {"namespace": root_namespace, "name": root_name},
            "run": {"runId": root_run_id},
        }
    return facet


def execution_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    return {
        **base("ExecutionParametersRunFacet.json"),
        "parameters": [
            {"key": key, "value": json_parameter_value(value)}
            for key, value in sorted(parameters.items())
        ],
    }


def json_parameter_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def processing_engine(name: str, version: str, adapter_version: str) -> dict[str, Any]:
    return {
        **base("ProcessingEngineRunFacet.json", "1-1-1"),
        "name": name,
        "version": version,
        "openlineageAdapterVersion": adapter_version,
    }


def error_message(message: str, programming_language: str = "python") -> dict[str, Any]:
    return {
        **base("ErrorMessageRunFacet.json", "1-0-1"),
        "message": message[:4000],
        "programmingLanguage": programming_language,
    }


def output_statistics(*, row_count: int, size: int, file_count: int) -> dict[str, Any]:
    return {
        **base("OutputStatisticsOutputDatasetFacet.json", "1-0-2"),
        "rowCount": row_count,
        "size": size,
        "fileCount": file_count,
    }


def column_lineage(mapping: dict[str, list[str]], input_dataset: dict[str, str]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for output_field, input_fields in mapping.items():
        fields[output_field] = {
            "inputFields": [
                {
                    "namespace": input_dataset["namespace"],
                    "name": input_dataset["name"],
                    "field": field,
                    "transformations": [
                        {
                            "type": "DIRECT" if field == output_field else "INDIRECT",
                            "subtype": "IDENTITY" if field == output_field else "TRANSFORMATION",
                        }
                    ],
                }
                for field in input_fields
            ]
        }
    return {**base("ColumnLineageDatasetFacet.json", "1-2-0"), "fields": fields}
