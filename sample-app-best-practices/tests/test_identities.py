from __future__ import annotations

import uuid

import pytest

from lineage_demo.identities import (
    DatasetIdentity,
    canonical_s3_dataset,
    parse_s3_uri,
    root_run_id,
    staged_identity,
    uuid7,
)


def test_uuid7_has_expected_version_and_variant() -> None:
    value = uuid7()
    assert value.version == 7
    assert value.variant == uuid.RFC_4122


def test_native_kfp_uuid_is_preserved() -> None:
    native = "3a6e0886-40b3-4cee-bddd-c983fd693155"
    assert root_run_id(native) == native


def test_non_uuid_kfp_id_maps_deterministically() -> None:
    assert root_run_id("pipeline/run/42") == root_run_id("pipeline/run/42")
    assert root_run_id("pipeline/run/42") != root_run_id("pipeline/run/43")


def test_s3_and_s3a_normalize_to_one_dataset_identity() -> None:
    expected = DatasetIdentity("s3://sample-data", "raw/documents.csv")
    assert canonical_s3_dataset("s3://sample-data/raw/documents.csv") == expected
    assert canonical_s3_dataset("s3a://sample-data/raw/documents.csv") == expected


def test_staged_identity_is_attempt_specific() -> None:
    first = staged_identity("sample-data", "asset", "run-1")
    second = staged_identity("sample-data", "asset", "run-2")
    assert first != second
    assert first.name == "staging/asset/run-1/documents.csv"


@pytest.mark.parametrize("uri", ["https://bucket/key", "s3://bucket", "s3:///key"])
def test_invalid_s3_identity_is_rejected(uri: str) -> None:
    with pytest.raises(ValueError):
        parse_s3_uri(uri)
