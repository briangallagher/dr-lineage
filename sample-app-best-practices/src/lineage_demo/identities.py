"""Canonical OpenLineage identity helpers.

Dataset identity in OpenLineage is the complete ``(namespace, name)`` pair.  All
emitters call these helpers so a spelling or URI-scheme difference cannot silently
split the graph.
"""

from __future__ import annotations

import secrets
import time
import uuid
from dataclasses import dataclass
from urllib.parse import urlparse

ROOT_UUID_NAMESPACE = uuid.UUID("f45380b1-cb55-4f8b-b390-baa5c8ec301f")


@dataclass(frozen=True)
class DatasetIdentity:
    namespace: str
    name: str

    def as_dict(self) -> dict[str, str]:
        return {"namespace": self.namespace, "name": self.name}


def uuid7() -> uuid.UUID:
    """Create an RFC 9562 UUIDv7 using only the Python standard library."""

    timestamp_ms = int(time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    value = timestamp_ms << 80
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    return uuid.UUID(int=value)


def root_run_id(pipeline_job_id: str) -> str:
    """Use a native KFP UUID where possible; otherwise map it deterministically."""

    try:
        return str(uuid.UUID(pipeline_job_id))
    except (ValueError, TypeError, AttributeError):
        return str(uuid.uuid5(ROOT_UUID_NAMESPACE, pipeline_job_id))


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError(f"Expected s3:// or s3a:// URI, got {uri!r}")
    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError(f"S3 URI must include an object or prefix key: {uri!r}")
    return parsed.netloc, key


def canonical_s3_dataset(uri: str) -> DatasetIdentity:
    bucket, key = parse_s3_uri(uri)
    return DatasetIdentity(namespace=f"s3://{bucket}", name=key.rstrip("/"))


def s3_uri(identity: DatasetIdentity) -> str:
    if not identity.namespace.startswith("s3://"):
        raise ValueError(f"Not an S3 dataset identity: {identity}")
    return f"{identity.namespace}/{identity.name}"


def staged_identity(bucket: str, asset_id: str, ingest_run_id: str) -> DatasetIdentity:
    return DatasetIdentity(
        namespace=f"s3://{bucket}",
        name=f"staging/{asset_id}/{ingest_run_id}/documents.csv",
    )


def transformed_identity(bucket: str, asset_id: str, spark_run_id: str) -> DatasetIdentity:
    return DatasetIdentity(
        namespace=f"s3://{bucket}",
        name=f"transformed/{asset_id}/{spark_run_id}",
    )


def embeddings_identity(bucket: str, asset_id: str, run_id: str) -> DatasetIdentity:
    return DatasetIdentity(
        namespace=f"s3://{bucket}",
        name=f"embeddings/{asset_id}/{run_id}/vectors.jsonl",
    )
