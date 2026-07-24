"""Contract integrity tests — these run in CI and gate every schema change."""

import pytest
from jsonschema import Draft202012Validator

from contracts.registry import ContractRegistry

registry = ContractRegistry()
ALL = [(et, v) for et in registry.event_types() for v in registry.versions(et)]

ENVELOPE_FIELDS = {"event_id", "event_type", "event_version", "occurred_at", "user_id", "surface"}


@pytest.mark.parametrize("event_type,version", ALL)
def test_schema_is_valid_jsonschema(event_type, version):
    Draft202012Validator.check_schema(registry.schema(event_type, version))


@pytest.mark.parametrize("event_type,version", ALL)
def test_event_type_const_matches_filename(event_type, version):
    schema = registry.schema(event_type, version)
    assert schema["properties"]["event_type"]["const"] == event_type
    assert schema["properties"]["event_version"]["const"] == version


@pytest.mark.parametrize("event_type,version", ALL)
def test_envelope_fields_present_and_required(event_type, version):
    schema = registry.schema(event_type, version)
    props = set(schema["properties"])
    required = set(schema["required"])
    assert ENVELOPE_FIELDS <= props, f"{event_type} missing envelope fields"
    assert ENVELOPE_FIELDS <= required, f"{event_type} envelope fields not required"


@pytest.mark.parametrize("event_type,version", ALL)
def test_additional_properties_closed(event_type, version):
    # Closed contracts are what make schema-drift detection possible at all.
    assert registry.schema(event_type, version)["additionalProperties"] is False


def test_registry_loaded_all_seven_types():
    assert set(registry.event_types()) == {
        "ad_click", "ad_impression", "follow", "like",
        "message_send", "post_create", "session_start",
    }