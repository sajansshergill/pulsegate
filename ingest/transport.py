"""Message-bus abstraction.

The ingest layer talks to a ``MessageBus``, not to Kafka directly. ``InMemoryBus``
runs the whole pipeline in-process (used by every test and by ``make demo``);
``KafkaBus`` is the production adapter. Swapping one for the other changes no
downstream code — bronze, producer, and triage are transport-agnostic.
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Message:
    topic: str
    value: dict
    key: str | None
    offset: int
    ts: float


class MessageBus(ABC):
    @abstractmethod
    def produce(self, topic: str, value: dict, key: str | None = None) -> int:
        """Append a message to a topic. Returns its offset."""

    @abstractmethod
    def read(self, topic: str, offset: int = 0) -> list[Message]:
        """Read all messages on a topic from ``offset`` onward (inclusive)."""


class InMemoryBus(MessageBus):
    """Ordered, offset-addressed, append-only log per topic — Kafka semantics,
    minus the network. Deterministic, so tests can assert exact offsets."""

    def __init__(self) -> None:
        self._topics: dict[str, list[Message]] = {}

    def produce(self, topic: str, value: dict, key: str | None = None) -> int:
        log = self._topics.setdefault(topic, [])
        offset = len(log)
        log.append(Message(topic=topic, value=value, key=key, offset=offset, ts=time.time()))
        return offset

    def read(self, topic: str, offset: int = 0) -> list[Message]:
        return list(self._topics.get(topic, [])[offset:])

    def topics(self) -> list[str]:
        return sorted(self._topics)

    def size(self, topic: str) -> int:
        return len(self._topics.get(topic, []))


class KafkaBus(MessageBus):
    """Production adapter over confluent-kafka.

    Import-guarded so the package works without the optional dependency
    installed. Designed to be a drop-in for InMemoryBus; not exercised in the
    local test suite (requires a running broker).
    """

    def __init__(self, bootstrap_servers: str, group_id: str = "pulsegate-bronze"):
        try:
            from confluent_kafka import Consumer, Producer  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "KafkaBus requires the 'confluent-kafka' extra: pip install pulsegate[kafka]"
            ) from exc
        from confluent_kafka import Consumer, Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._consumer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self._Consumer = Consumer

    def produce(self, topic: str, value: dict, key: str | None = None) -> int:  # pragma: no cover
        self._producer.produce(
            topic, key=key, value=json.dumps(value).encode("utf-8")
        )
        self._producer.poll(0)
        return -1  # Kafka assigns offsets server-side; not known synchronously

    def read(self, topic: str, offset: int = 0) -> list[Message]:  # pragma: no cover
        consumer = self._Consumer(self._consumer_conf)
        consumer.subscribe([topic])
        out: list[Message] = []
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    continue
                out.append(Message(
                    topic=topic, value=json.loads(msg.value()),
                    key=msg.key().decode() if msg.key() else None,
                    offset=msg.offset(), ts=msg.timestamp()[1] / 1000,
                ))
            consumer.commit()
        finally:
            consumer.close()
        return out