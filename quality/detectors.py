"""Detectors.

Each detector is a pure function of (snapshot, baseline) -> list[Finding]. Purity
is deliberate: it makes every detector unit-testable with crafted inputs and keeps
detection logic independent of where the data came from. Severity scales with the
size of the deviation, not just its presence — a 5% volume wobble is not a page.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from quality.baselines import Baseline, Snapshot

INFO, WARNING, CRITICAL = "info", "warning", "critical"


@dataclass
class Finding:
    detector: str
    signal: str
    severity: str
    event_type: str | None
    detail: str
    evidence: dict = field(default_factory=dict)


class Detector(ABC):
    name: str = "detector"

    @abstractmethod
    def run(self, snap: Snapshot, base: Baseline) -> list[Finding]: ...


def _sev(magnitude: float, warn: float, crit: float) -> str:
    return CRITICAL if magnitude >= crit else WARNING if magnitude >= warn else INFO


class VolumeAnomalyDetector(Detector):
    """Flags an event type whose share of volume has drifted far from baseline."""
    name = "volume_anomaly"

    def __init__(self, warn: float = 0.4, crit: float = 0.8):
        self.warn, self.crit = warn, crit

    def run(self, snap, base):
        findings = []
        current = snap.volume_share
        for et, base_share in base.volume_share.items():
            if base_share <= 0:
                continue
            cur = current.get(et, 0.0)
            rel = abs(cur - base_share) / base_share
            sev = _sev(rel, self.warn, self.crit)
            if sev == INFO:
                continue
            direction = "dropped" if cur < base_share else "spiked"
            findings.append(Finding(
                self.name, f"volume_{direction}", sev, et,
                f"{et} volume {direction} {rel:.0%} vs baseline",
                {"baseline_share": round(base_share, 4), "current_share": round(cur, 4)},
            ))
        return findings


class NullRateDetector(Detector):
    """Flags a monitored column whose null rate rose materially above baseline."""
    name = "null_rate"

    def __init__(self, warn: float = 0.05, crit: float = 0.25):
        self.warn, self.crit = warn, crit

    def run(self, snap, base):
        findings = []
        for et, cols in snap.null_rates.items():
            base_cols = base.null_rates.get(et, {})
            for col, cur in cols.items():
                delta = cur - base_cols.get(col, 0.0)
                sev = _sev(delta, self.warn, self.crit)
                if sev == INFO:
                    continue
                findings.append(Finding(
                    self.name, "null_spike", sev, et,
                    f"{et}.{col} null rate up {delta:.0%}",
                    {"column": col, "current_null_rate": round(cur, 4)},
                ))
        return findings


class DuplicateEventDetector(Detector):
    """event_id is meant to be unique; any duplicate is a correctness bug."""
    name = "duplicate_event"

    def run(self, snap, base):
        if snap.duplicate_event_ids <= 0:
            return []
        return [Finding(
            self.name, "duplicate_event_id", CRITICAL, None,
            f"{snap.duplicate_event_ids} duplicate event_id(s) in bronze",
            {"duplicate_count": snap.duplicate_event_ids},
        )]


class DeadLetterRateDetector(Detector):
    """Flags when the share of events failing the contract gate rises."""
    name = "dead_letter_rate"

    def __init__(self, warn: float = 0.05, crit: float = 0.15):
        self.warn, self.crit = warn, crit

    def run(self, snap, base):
        delta = snap.dead_letter_rate - base.dead_letter_rate
        sev = _sev(delta, self.warn, self.crit)
        if sev == INFO:
            return []
        return [Finding(
            self.name, "reject_rate_up", sev, None,
            f"dead-letter rate {snap.dead_letter_rate:.0%} vs baseline {base.dead_letter_rate:.0%}",
            {"baseline_rate": round(base.dead_letter_rate, 4),
             "current_rate": round(snap.dead_letter_rate, 4),
             "by_fault": snap.dead_letter_by_fault},
        )]


class SchemaDriftDetector(Detector):
    """Extra-field rejections mean producers are emitting fields not in the
    contract — schema drift caught at the boundary."""
    name = "schema_drift"

    def run(self, snap, base):
        drift = snap.dead_letter_by_fault.get("extra_field", 0)
        if drift <= 0:
            return []
        sev = CRITICAL if drift >= 50 else WARNING
        return [Finding(
            self.name, "unexpected_field", sev, None,
            f"{drift} event(s) rejected for fields outside the contract",
            {"extra_field_rejections": drift},
        )]


class ReferentialIntegrityDetector(Detector):
    """Flags child events whose foreign key has no matching parent event.

    Pure over key sets so it's testable without a live referential feed; wiring
    it to real parent/child keys from bronze is a generator enhancement.
    """
    name = "referential_integrity"

    def __init__(self, child_type: str, parent_type: str,
                 child_keys: set[str], parent_keys: set[str],
                 warn: float = 0.02, crit: float = 0.1):
        self.child_type, self.parent_type = child_type, parent_type
        self.child_keys, self.parent_keys = child_keys, parent_keys
        self.warn, self.crit = warn, crit

    def run(self, snap, base):
        if not self.child_keys:
            return []
        orphans = self.child_keys - self.parent_keys
        rate = len(orphans) / len(self.child_keys)
        sev = _sev(rate, self.warn, self.crit)
        if sev == INFO:
            return []
        return [Finding(
            self.name, "orphaned_event", sev, self.child_type,
            f"{len(orphans)} {self.child_type} without matching {self.parent_type} ({rate:.0%})",
            {"orphan_count": len(orphans), "child_total": len(self.child_keys)},
        )]


DEFAULT_DETECTORS: list[Detector] = [
    VolumeAnomalyDetector(),
    NullRateDetector(),
    DuplicateEventDetector(),
    DeadLetterRateDetector(),
    SchemaDriftDetector(),
]