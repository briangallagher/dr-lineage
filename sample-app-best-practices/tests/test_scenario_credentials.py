from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lineage_demo.scenarios import read_credentials, run_pipeline


def test_scenario_credentials_are_read_from_stdin() -> None:
    assert read_credentials(StringIO("token\naccess\nsecret\n")) == (
        "token",
        "access",
        "secret",
    )


def test_scenario_credentials_require_all_values() -> None:
    with pytest.raises(ValueError, match="S3 secret key"):
        read_credentials(StringIO("token\naccess\n"))


def test_run_pipeline_uses_and_verifies_pipeline_version_reference() -> None:
    client = Mock()
    client.run_pipeline.return_value = SimpleNamespace(run_id="run-1")
    client.wait_for_run_completion.return_value = SimpleNamespace(
        state=SimpleNamespace(value="SUCCEEDED")
    )
    client.get_run.return_value = SimpleNamespace(
        pipeline_version_reference=SimpleNamespace(
            pipeline_id="pipeline-1",
            pipeline_version_id="version-1",
        )
    )

    result = run_pipeline(
        client,
        experiment_id="experiment-1",
        pipeline_id="pipeline-1",
        pipeline_version_id="version-1",
        namespace="ol-best-practices",
        asset_id="asset-1",
        failure_mode="none",
        scenario="success-1",
        timeout=60,
    )

    client.run_pipeline.assert_called_once()
    submitted = client.run_pipeline.call_args.kwargs
    assert submitted["experiment_id"] == "experiment-1"
    assert submitted["job_name"].startswith("openlineage-success-1-")
    assert submitted["params"] == {"asset_id": "asset-1", "failure_mode": "none"}
    assert submitted["pipeline_id"] == "pipeline-1"
    assert submitted["version_id"] == "version-1"
    assert submitted["enable_caching"] is False
    assert submitted["service_account"] == "pipeline-runner-dspa"
    client.get_run.assert_called_once_with("run-1")
    assert result.pipeline_id == "pipeline-1"
    assert result.pipeline_version_id == "version-1"
