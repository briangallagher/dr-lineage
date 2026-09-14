from __future__ import annotations

from lineage_demo.conformance import (
    AREA_SPECS,
    STATUS_PROVEN,
    Fixture,
    _check_facets,
    _check_hierarchy,
    _check_lifecycle,
    semantic_projection,
)


def test_fixture_is_deterministic_for_a_suite_id() -> None:
    first = Fixture("unit-suite").events()
    second = Fixture("unit-suite").events()

    assert first == second
    assert Fixture("other-suite").events() != first


def test_fixture_contains_registry_bridge_and_distinct_retry_identities() -> None:
    fixture = Fixture("unit-suite")
    projection = semantic_projection(fixture.events())

    registry_link = (
        (fixture.registry_namespace, fixture.asset_name),
        (fixture.physical_namespace, fixture.raw_name),
    )
    assert registry_link in projection["symlinks"]
    assert fixture._run("failed-attempt-1", 40000001) != fixture._run("failed-attempt-2", 40000002)
    assert fixture.root_success in projection["runs"]
    assert fixture.root_failure in projection["runs"]


def test_pure_semantic_checks_pass_against_fixture() -> None:
    fixture = Fixture("unit-suite")
    events = fixture.events()

    assert _check_hierarchy(_FakeBackend(), fixture, events).status == STATUS_PROVEN
    assert _check_lifecycle(_FakeBackend(), fixture, events).status == STATUS_PROVEN
    assert _check_facets(_FakeBackend(), fixture, events).status == STATUS_PROVEN


def test_every_documented_area_has_plain_language_risk_and_mitigation() -> None:
    assert len(AREA_SPECS) == 14
    for area in AREA_SPECS.values():
        assert area.simple_meaning
        assert area.why_significant
        assert area.significance in {"Critical", "High", "Medium", "Low"}
        assert area.default_gap
        assert area.mitigation


class _FakeBackend:
    name = "fixture"
