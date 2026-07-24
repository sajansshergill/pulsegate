"""Bronze-layer end-to-end tests."""

import json

import pytest

from ingest.bronze import BronzeWriter
from ingest.producer import IngestProducer
from ingest.transport import InMemoryBus


@pytest.fixture
def pipeline(tmp_path):
    bus = InMemoryBus()
    producer = IngestProducer(bus)
    stats = producer.run_from_generator(count=300, corrupt_rate=0.2, seed=42)
    bronze = BronzeWriter(str(tmp_path / "bronze.duckdb"))
    written = bronze.consume(bus)
    yield stats, bronze, written, bus
    bronze.close()


def test_bronze_reconciles_with_producer(pipeline):
    stats, bronze, _, _ = pipeline
    counts = bronze.counts()
    assert counts["bronze_events"] == stats.accepted
    assert counts["dead_letter"] == stats.rejected
    assert counts["bronze_events"] + counts["dead_letter"] == stats.total == 300


def test_reconsume_is_idempotent(pipeline):
    _, bronze, first, bus = pipeline
    before = bronze.counts()
    second = bronze.consume(bus)                      # replay same offsets
    assert second == {"bronze_events": 0, "dead_letter": 0}
    assert bronze.counts() == before                 # no duplicates


def test_payload_round_trips(pipeline):
    _, bronze, _, _ = pipeline
    row = bronze.con.execute(
        "SELECT event_type, payload FROM bronze_events LIMIT 1"
    ).fetchone()
    payload = json.loads(row[1])
    assert payload["event_type"] == row[0]
    assert "event_id" in payload


def test_dead_letter_carries_errors(pipeline):
    _, bronze, _, _ = pipeline
    errors, payload = bronze.con.execute(
        "SELECT errors, payload FROM dead_letter LIMIT 1"
    ).fetchone()
    assert json.loads(errors)                         # non-empty error list
    assert json.loads(payload)                        # raw payload retained