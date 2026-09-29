import base64
import hashlib
from pathlib import Path
from unittest.mock import Mock, call

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from lineage_demo.seed import add_lifecycle_content_md5, ensure_bucket, seed_source


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


def test_lifecycle_checksum_header_matches_request_body() -> None:
    body = b"<LifecycleConfiguration>example</LifecycleConfiguration>"
    request = Mock(body=body, headers={})

    add_lifecycle_content_md5(request)

    expected = base64.b64encode(hashlib.md5(body, usedforsecurity=False).digest()).decode("ascii")
    assert request.headers["Content-MD5"] == expected


def test_botocore_signs_lifecycle_request_with_content_md5() -> None:
    class RequestCaptured(Exception):
        pass

    captured: dict[str, object] = {}

    def capture(request: object, **_: object) -> None:
        captured["body"] = request.body
        captured["header"] = request.headers.get("Content-MD5")
        raise RequestCaptured

    client = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 0}),
    )
    client.meta.events.register(
        "before-sign.s3.PutBucketLifecycleConfiguration", add_lifecycle_content_md5
    )
    client.meta.events.register("before-send.s3.PutBucketLifecycleConfiguration", capture)

    with pytest.raises(RequestCaptured):
        client.put_bucket_lifecycle_configuration(
            Bucket="sample-data",
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-staging-after-7-days",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "staging/"},
                        "Expiration": {"Days": 7},
                    }
                ]
            },
        )

    body = captured["body"]
    assert isinstance(body, bytes)
    expected = base64.b64encode(hashlib.md5(body, usedforsecurity=False).digest()).decode("ascii")
    assert captured["header"] == expected.encode("ascii")
