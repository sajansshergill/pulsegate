"""Triage engine end-to-end tests, driven through the real pipeline."""

import random

import pytest

from generator.generate import EVENT_TYPES, corrupt, generate_event
from ingest.bronze import BronzeWriter
from ingest.producer import IngestProducer
from ingest.transport import InMemoryBus
from quality.baselines import compute_baseline
from quality.triage import TriageEngine


def _run_batch(db_path, count, corrupt_rate, seed, skip_type=None):
    """Drive a batch through gate -> bronze and return the bronze writer."""
    bus = InMemoryBus()
    producer = IngestProducer(bus)
    random.seed(seed)
    pool = [t for t in EVENT_TYPES if t != skip_type] if skip_type else EVENT_TYPES
    for _ in range(count):
        event = generate_event(random.choice(pool))
        fault = None
        if random.random() < corrupt_rate:
            event, fault = corrupt(event)
        producer.ingest(event, fault_kind=fault)
    bronze = BronzeWriter(str(db_path))
    bronze.consume(bus)
    return bronze


def test_healthy_data_produces_no_findings(tmp_path):
    bronze = _run_batch(tmp_path / "a.duckdb", 800, corrupt_rate=0.0, seed=1)
    baseline = compute_baseline(bronze.con)
    records = TriageEngine(bronze.con).run(baseline)
    assert records == []
    bronze.close()


@pytest.fixture
def anomalous(tmp_path):
    base_bronze = _run_batch(tmp_path / "base.duckdb", 1000, corrupt_rate=0.0, seed=7)
    baseline = compute_baseline(base_bronze.con)
    base_bronze.close()
    cur_bronze = _run_batch(tmp_path / "cur.duckdb", 1000, corrupt_rate=0.3, seed=9,
                            skip_type="follow")
    engine = TriageEngine(cur_bronze.con)
    records = engine.run(baseline)
    yield records, engine, baseline
    cur_bronze.close()


def test_volume_drop_detected(anomalous):
    records, _, _ = anomalous
    follow = [r for r in records if r.event_type == "follow"]
    assert follow and follow[0].signal == "volume_dropped"
    assert follow[0].suggested_owner == "ingest-oncall"


def test_reject_rate_spike_detected(anomalous):
    records, _, _ = anomalous
    assert any(r.detector == "dead_letter_rate" for r in records)


def test_schema_drift_detected(anomalous):
    records, _, _ = anomalous
    assert any(r.detector == "schema_drift" for r in records)


def test_records_sorted_critical_first(anomalous):
    records, _, _ = anomalous
    ranks = {"info": 0, "warning": 1, "critical": 2}
    idxs = [ranks[r.severity] for r in records]
    assert idxs == sorted(idxs, reverse=True)


def test_every_record_has_an_owner(anomalous):
    records, _, _ = anomalous
    assert all(r.suggested_owner for r in records)


def test_rerun_is_idempotent(anomalous):
    _, engine, baseline = anomalous
    before = len(engine.open_records())
    engine.run(baseline)
    after = len(engine.open_records())
    assert after == before