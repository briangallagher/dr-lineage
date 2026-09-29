from pathlib import Path
from unittest.mock import Mock, call

import pytest
from botocore.exceptions import ClientError

from lineage_demo.seed import ensure_bucket, seed_source


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "HeadBucket")


def test_seed_creates_missing_buckets_and_preserves_other_lifecycle_rules(tmp_path: Path) -> None:
    client = Mock()
    client.head_bucket.side_effect = [client_error("404"), None]
    client.get_bucket_lifecycle_configuration.return_value = {
        "Rules": [
            {"ID": "unrelated-rule", "Status": "Enabled", "Filter": {"Prefix": "other/"}},
            {"ID": "expire-staging-after-7-days", "Status": "Disabled"},
        ]
    }
    source = tmp_path / "source-v1.csv"
    source.write_bytes(b"title,text\nexample,row\n")

    seed_source(client, source)

    client.head_bucket.assert_has_calls(
        [call(Bucket="sample-data"), call(Bucket="pipeline-artifacts")]
    )
    client.create_bucket.assert_called_once_with(Bucket="sample-data")
    client.put_bucket_versioning.assert_called_once_with(
        Bucket="sample-data", VersioningConfiguration={"Status": "Suspended"}
    )
    client.put_object.assert_called_once_with(
        Bucket="sample-data", Key="raw/documents.csv", Body=source.read_bytes()
    )
    rules = client.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"][
        "Rules"
    ]
    assert [rule["ID"] for rule in rules] == [
        "unrelated-rule",
        "expire-staging-after-7-days",
    ]
    assert rules[1]["Expiration"] == {"Days": 7}


def test_ensure_bucket_does_not_hide_authorization_errors() -> None:
    client = Mock()
    client.head_bucket.side_effect = client_error("403")

    with pytest.raises(ClientError):
        ensure_bucket(client, "sample-data")

    client.create_bucket.assert_not_called()


def test_seed_adds_lifecycle_rule_when_bucket_has_none(tmp_path: Path) -> None:
    client = Mock()
    client.get_bucket_lifecycle_configuration.side_effect = client_error(
        "NoSuchLifecycleConfiguration"
    )
    source = tmp_path / "source-v1.csv"
    source.write_bytes(b"source\n")

    seed_source(client, source)

    rules = client.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"][
        "Rules"
    ]
    assert [rule["ID"] for rule in rules] == ["expire-staging-after-7-days"]
