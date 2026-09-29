"""Seed the isolated POC object store using the app's pinned S3 client."""

from __future__ import annotations

import argparse
from pathlib import Path

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from lineage_demo.config import get_settings
from lineage_demo.storage import s3_client


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def ensure_bucket(client: BaseClient, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        if _error_code(error) not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)


def seed_source(client: BaseClient, source: Path) -> None:
    for bucket in ("sample-data", "pipeline-artifacts"):
        ensure_bucket(client, bucket)

    client.put_bucket_versioning(
        Bucket="sample-data", VersioningConfiguration={"Status": "Suspended"}
    )
    client.put_object(Bucket="sample-data", Key="raw/documents.csv", Body=source.read_bytes())

    try:
        existing = client.get_bucket_lifecycle_configuration(Bucket="sample-data").get("Rules", [])
    except ClientError as error:
        if _error_code(error) != "NoSuchLifecycleConfiguration":
            raise
        existing = []
    rule = {
        "ID": "expire-staging-after-7-days",
        "Status": "Enabled",
        "Filter": {"Prefix": "staging/"},
        "Expiration": {"Days": 7},
    }
    rules = [item for item in existing if item.get("ID") != rule["ID"]]
    rules.append(rule)
    client.put_bucket_lifecycle_configuration(
        Bucket="sample-data", LifecycleConfiguration={"Rules": rules}
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    args = parser.parse_args()
    seed_source(s3_client(get_settings()), args.file)
    print("Seeded sample-data/raw/documents.csv and pipeline-artifacts")


if __name__ == "__main__":
    main()
