"""Detector unit tests — pure, no bronze needed."""

from quality.baselines import Baseline, Snapshot
from quality.detectors import (
    DeadLetterRateDetector, DuplicateEventDetector, NullRateDetector,
    ReferentialIntegrityDetector, SchemaDriftDetector, VolumeAnomalyDetector,
)


def _snap(**kw):
    base = dict(total=1000, volume_by_type={"like": 500, "follow": 500},
                null_rates={}, duplicate_event_ids=0,
                dead_letter_total=0, dead_letter_by_fault={})
    base.update(kw)
    return Snapshot(**base)


def _base(**kw):
    base = dict(volume_share={"like": 0.5, "follow": 0.5}, null_rates={},
                dead_letter_rate=0.0, sample_size=1000)
    base.update(kw)
    return Baseline(**base)


def test_volume_stable_produces_nothing():
    assert VolumeAnomalyDetector().run(_snap(), _base()) == []


def test_volume_collapse_is_flagged():
    snap = _snap(total=1000, volume_by_type={"like": 950, "follow": 50})
    findings = VolumeAnomalyDetector().run(snap, _base())
    follow = [f for f in findings if f.event_type == "follow"]
    assert follow and follow[0].signal == "volume_dropped"
    assert follow[0].severity == "critical"        # 90% drop


def test_null_spike_is_flagged():
    snap = _snap(null_rates={"like": {"user_id": 0.3}})
    base = _base(null_rates={"like": {"user_id": 0.0}})
    findings = NullRateDetector().run(snap, base)
    assert findings and findings[0].severity == "critical"


def test_duplicate_event_is_critical():
    findings = DuplicateEventDetector().run(_snap(duplicate_event_ids=4), _base())
    assert findings and findings[0].severity == "critical"


def test_dead_letter_rate_spike_flagged():
    snap = _snap(total=800, dead_letter_total=200)   # 20% reject rate
    findings = DeadLetterRateDetector().run(snap, _base(dead_letter_rate=0.02))
    assert findings and findings[0].severity == "critical"


def test_schema_drift_from_extra_fields():
    snap = _snap(dead_letter_by_fault={"extra_field": 60})
    findings = SchemaDriftDetector().run(snap, _base())
    assert findings and findings[0].severity == "critical"


def test_referential_integrity_all_matched():
    d = ReferentialIntegrityDetector("ad_click", "ad_impression",
                                     child_keys={"a", "b"}, parent_keys={"a", "b", "c"})
    assert d.run(_snap(), _base()) == []


def test_referential_integrity_orphans_flagged():
    d = ReferentialIntegrityDetector("ad_click", "ad_impression",
                                     child_keys={"a", "b", "x", "y"}, parent_keys={"a", "b"})
    findings = d.run(_snap(), _base())
    assert findings and findings[0].signal == "orphaned_event"
    assert findings[0].evidence["orphan_count"] == 2