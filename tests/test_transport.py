"""In-memory transport tests (Kafka semantics without a broker)."""

from ingest.transport import InMemoryBus


def test_offsets_increment_per_topic():
    bus = InMemoryBus()
    assert bus.produce("t", {"a": 1}) == 0
    assert bus.produce("t", {"a": 2}) == 1
    assert bus.produce("t", {"a": 3}) == 2


def test_topics_are_isolated():
    bus = InMemoryBus()
    bus.produce("t1", {"x": 1})
    bus.produce("t2", {"y": 2})
    assert bus.size("t1") == 1 and bus.size("t2") == 1
    assert bus.read("t1")[0].value == {"x": 1}


def test_read_from_offset():
    bus = InMemoryBus()
    for i in range(5):
        bus.produce("t", {"i": i})
    tail = bus.read("t", offset=3)
    assert [m.value["i"] for m in tail] == [3, 4]


def test_ordering_and_keys_preserved():
    bus = InMemoryBus()
    bus.produce("t", {"i": 0}, key="u_1")
    bus.produce("t", {"i": 1}, key="u_2")
    msgs = bus.read("t")
    assert [m.key for m in msgs] == ["u_1", "u_2"]
    assert [m.offset for m in msgs] == [0, 1]