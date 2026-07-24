"""Backward-compatibility gate tests.

The registry only ships v1 today, so these tests exercise the compat checker
against crafted old/new pairs — the same logic CI runs when someone proposes a
vN+1 of a contract.
"""

from contracts.registry import ContractRegistry, check_backward_compatible

registry = ContractRegistry()


def _base():
    return {
        "type": "object",
        "required": ["event_id", "caption_length"],
        "properties": {
            "event_id": {"type": "string"},
            "caption_length": {"type": "integer"},
            "content_type": {"enum": ["photo", "video", "text"]},
        },
    }


def test_identity_is_compatible():
    assert check_backward_compatible(_base(), _base()) == []


def test_adding_optional_field_is_compatible():
    new = _base()
    new["properties"]["alt_text"] = {"type": "string"}  # optional addition
    assert check_backward_compatible(_base(), new) == []


def test_removing_required_field_is_breaking():
    new = _base()
    del new["properties"]["caption_length"]
    new["required"] = ["event_id"]
    breaks = check_backward_compatible(_base(), new)
    assert any("required field removed: caption_length" in b for b in breaks)


def test_downgrading_required_to_optional_is_breaking():
    new = _base()
    new["required"] = ["event_id"]  # caption_length no longer required
    breaks = check_backward_compatible(_base(), new)
    assert any("downgraded to optional: caption_length" in b for b in breaks)


def test_type_change_is_breaking():
    new = _base()
    new["properties"]["caption_length"]["type"] = "string"
    breaks = check_backward_compatible(_base(), new)
    assert any("type changed for caption_length" in b for b in breaks)


def test_dropping_enum_value_is_breaking():
    new = _base()
    new["properties"]["content_type"]["enum"] = ["photo", "video"]  # dropped "text"
    breaks = check_backward_compatible(_base(), new)
    assert any("enum values dropped for content_type" in b for b in breaks)


def test_shipped_v1_schemas_are_self_compatible():
    for et in registry.event_types():
        schema = registry.schema(et)
        assert check_backward_compatible(schema, schema) == []