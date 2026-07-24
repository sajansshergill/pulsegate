"""PulseGate contract registry.

Loads versioned event contracts (JSON Schema) from ``registry/`` and enforces
them at the producer boundary. This is the quality gate the rest of the platform
depends on: nothing reaches bronze without passing through here.

Filename convention:  ``{event_type}.v{version}.json``  e.g. ``like.v1.json``
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator

_FILENAME = re.compile(r"^(?P<event_type>[a-z_]+)\.v(?P<version>\d+)\.json$")
_REGISTRY_DIR = Path(__file__).parent / "registry"


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    event_type: str
    version: int
    errors: list[str]


class ContractRegistry:
    """In-memory index of every event contract, keyed by (event_type, version)."""

    def __init__(self, registry_dir: Path | str = _REGISTRY_DIR):
        self.registry_dir = Path(registry_dir)
        self._schemas: dict[tuple[str, int], dict] = {}
        self._load()

    def _load(self) -> None:
        for path in sorted(self.registry_dir.glob("*.json")):
            m = _FILENAME.match(path.name)
            if not m:
                raise ValueError(f"registry file does not follow naming convention: {path.name}")
            event_type = m.group("event_type")
            version = int(m.group("version"))
            schema = json.loads(path.read_text())
            # Fail fast if a contract is not itself a valid JSON Schema.
            Draft202012Validator.check_schema(schema)
            self._schemas[(event_type, version)] = schema

    # ---- introspection -------------------------------------------------
    def event_types(self) -> list[str]:
        return sorted({et for et, _ in self._schemas})

    def versions(self, event_type: str) -> list[int]:
        return sorted(v for et, v in self._schemas if et == event_type)

    def latest_version(self, event_type: str) -> int:
        versions = self.versions(event_type)
        if not versions:
            raise KeyError(f"unknown event_type: {event_type}")
        return versions[-1]

    def schema(self, event_type: str, version: int | None = None) -> dict:
        version = version or self.latest_version(event_type)
        return self._schemas[(event_type, version)]

    # ---- enforcement ---------------------------------------------------
    def validate(self, event: dict, version: int | None = None) -> ValidationResult:
        """Validate one event against its contract.

        The event_type is read from the event itself; unknown types are rejected
        (this is what stops orphaned / mislabelled events at the boundary).
        """
        event_type = event.get("event_type")
        if not isinstance(event_type, str) or event_type not in self.event_types():
            return ValidationResult(False, str(event_type), -1,
                                    [f"unknown or missing event_type: {event_type!r}"])
        version = version or self.latest_version(event_type)
        validator = Draft202012Validator(self._schemas[(event_type, version)])
        errors = [
            f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}"
            for e in sorted(validator.iter_errors(event), key=lambda e: list(e.path))
        ]
        return ValidationResult(not errors, event_type, version, errors)


def check_backward_compatible(old_schema: dict, new_schema: dict) -> list[str]:
    """Return a list of breaking changes going from ``old_schema`` to ``new_schema``.

    Empty list == compatible. Wired into CI so a schema change that would break
    existing producers or downstream consumers blocks the merge. Rules enforced:

      * a previously-required field may not be removed or made optional-by-deletion
      * a field's declared type may not change
      * an enum may not drop previously-allowed values
    """
    breaks: list[str] = []

    old_req = set(old_schema.get("required", []))
    new_req = set(new_schema.get("required", []))
    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})

    for field in old_req:
        if field not in new_props:
            breaks.append(f"required field removed: {field}")
        elif field not in new_req:
            breaks.append(f"required field downgraded to optional: {field}")

    for field, old_def in old_props.items():
        new_def = new_props.get(field)
        if new_def is None:
            continue  # removal of an optional field is handled above if required
        if "type" in old_def and "type" in new_def and old_def["type"] != new_def["type"]:
            breaks.append(f"type changed for {field}: {old_def['type']} -> {new_def['type']}")
        if "enum" in old_def and "enum" in new_def:
            dropped = set(old_def["enum"]) - set(new_def["enum"])
            if dropped:
                breaks.append(f"enum values dropped for {field}: {sorted(dropped)}")

    return breaks