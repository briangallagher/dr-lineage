from __future__ import annotations

from pathlib import Path

import pytest

from lineage_demo.processing import _mock_vector, validate_source_csv

DATA = Path(__file__).parents[1] / "data"


def test_source_variants_have_no_identifier_and_are_valid() -> None:
    assert validate_source_csv((DATA / "source-v1.csv").read_bytes()) == 3
    assert validate_source_csv((DATA / "source-v2.csv").read_bytes()) == 4


def test_source_with_identifier_is_rejected() -> None:
    with pytest.raises(ValueError, match="no durable row identifier"):
        validate_source_csv(b"id,title,category,text\n1,A,B,C\n")


def test_mock_embeddings_are_deterministic_and_bounded() -> None:
    first = _mock_vector("same text")
    assert first == _mock_vector("same text")
    assert first != _mock_vector("different text")
    assert len(first) == 8
    assert all(-1 <= value <= 1 for value in first)
