from io import StringIO

import pytest

from lineage_demo.scenarios import read_credentials


def test_scenario_credentials_are_read_from_stdin() -> None:
    assert read_credentials(StringIO("token\naccess\nsecret\n")) == (
        "token",
        "access",
        "secret",
    )


def test_scenario_credentials_require_all_values() -> None:
    with pytest.raises(ValueError, match="S3 secret key"):
        read_credentials(StringIO("token\naccess\n"))
