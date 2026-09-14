"""S3-compatible object-storage operations used by the processing jobs."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import boto3
from botocore.client import BaseClient

from lineage_demo.config import Settings
from lineage_demo.identities import DatasetIdentity, canonical_s3_dataset, parse_s3_uri


def s3_client(settings: Settings) -> BaseClient:
    kwargs: dict[str, str] = {
        "endpoint_url": settings.s3_endpoint,
        "region_name": settings.s3_region,
    }
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
    if settings.aws_secret_access_key:
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    return boto3.client("s3", **kwargs)


def read_bytes(client: BaseClient, uri: str) -> bytes:
    bucket, key = parse_s3_uri(uri)
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def put_create_only(client: BaseClient, identity: DatasetIdentity, body: bytes) -> None:
    bucket = identity.namespace.removeprefix("s3://")
    client.put_object(Bucket=bucket, Key=identity.name, Body=body, IfNoneMatch="*")


def put_bytes(client: BaseClient, uri: str, body: bytes) -> None:
    identity = canonical_s3_dataset(uri)
    client.put_object(
        Bucket=identity.namespace.removeprefix("s3://"),
        Key=identity.name,
        Body=body,
    )


def list_objects(client: BaseClient, uri: str, suffix: str = "") -> Iterator[dict]:
    bucket, prefix = parse_s3_uri(uri)
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for item in page.get("Contents", []):
            if not suffix or item["Key"].endswith(suffix):
                yield {**item, "Bucket": bucket}


def download_object(client: BaseClient, *, bucket: str, key: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(destination))
