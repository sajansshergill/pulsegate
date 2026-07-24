"""Ingest producer routing tests."""

from generator.generate import generate_event
from ingest.producer import DEAD_LETTER_TOPIC, EVENTS_TOPIC, IngestProducer
from ingest.transport import InMemoryBus


def test_valid_event_routes_to_events_topic():
    bus = InMemoryBus()
    p = IngestProducer(bus)
    assert p.ingest(generate_event("like")) is True
    assert bus.size(EVENTS_TOPIC) == 1
    assert bus.size(DEAD_LETTER_TOPIC) == 0


def test_invalid_event_routes_to_dead_letter_with_errors():
    bus = InMemoryBus()
    p = IngestProducer(bus)
    bad = generate_event("like")
    bad["surface"] = "telepathy"
    assert p.ingest(bad, fault_kind="bad_enum") is False
    assert bus.size(EVENTS_TOPIC) == 0
    dl = bus.read(DEAD_LETTER_TOPIC)[0].value
    assert dl["fault_kind"] == "bad_enum"
    assert dl["errors"] and "surface" in dl["errors"][0]
    assert dl["payload"]["surface"] == "telepathy"   # raw payload preserved


def test_events_are_keyed_by_user():
    bus = InMemoryBus()
    p = IngestProducer(bus)
    e = generate_event("follow")
    p.ingest(e)
    assert bus.read(EVENTS_TOPIC)[0].key == e["user_id"]


def test_run_from_generator_stats_reconcile():
    bus = InMemoryBus()
    p = IngestProducer(bus)
    stats = p.run_from_generator(count=200, corrupt_rate=0.25, seed=11)
    assert stats.total == 200
    assert stats.accepted == bus.size(EVENTS_TOPIC)
    assert stats.rejected == bus.size(DEAD_LETTER_TOPIC)
    assert stats.rejected > 0            # fault injection actually produced rejects