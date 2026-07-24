"""Ingest producer — the enforcement point.

Every event is validated against its contract before it enters the pipeline.
Valid events go to ``events.raw`` keyed by user_id (so a user's events stay
ordered on one partition). Contract failures are *not* dropped — they're routed
to ``events.deadletter`` with the exact validation errors, which is the raw
material the phase-3 triage engine consumes.
"""

from __future__ import annotations

from dataclasses import dataclass

from contracts.registry import ContractRegistry
from generator.generate import corrupt, generate_event
from ingest.transport import MessageBus

EVENTS_TOPIC = "events.raw"
DEAD_LETTER_TOPIC = "events.deadletter"


@dataclass
class IngestStats:
    accepted: int = 0
    rejected: int = 0

    @property
    def total(self) -> int:
        return self.accepted + self.rejected


class IngestProducer:
    def __init__(
        self,
        bus: MessageBus,
        registry: ContractRegistry | None = None,
        events_topic: str = EVENTS_TOPIC,
        dead_letter_topic: str = DEAD_LETTER_TOPIC,
    ):
        self.bus = bus
        self.registry = registry or ContractRegistry()
        self.events_topic = events_topic
        self.dead_letter_topic = dead_letter_topic
        self.stats = IngestStats()

    def ingest(self, event: dict, fault_kind: str | None = None) -> bool:
        """Validate and route a single event. Returns True if accepted."""
        result = self.registry.validate(event)
        if result.ok:
            self.bus.produce(self.events_topic, value=event, key=event.get("user_id"))
            self.stats.accepted += 1
            return True

        self.bus.produce(self.dead_letter_topic, value={
            "event_type": result.event_type,
            "fault_kind": fault_kind,
            "errors": result.errors,
            "payload": event,
        }, key=event.get("user_id"))
        self.stats.rejected += 1
        return False

    def run_from_generator(
        self, count: int, corrupt_rate: float = 0.0, seed: int | None = None
    ) -> IngestStats:
        """Drive the pipeline from the simulated generator (used by demo + tests)."""
        import random
        if seed is not None:
            random.seed(seed)
        for _ in range(count):
            event = generate_event()
            fault = None
            if random.random() < corrupt_rate:
                event, fault = corrupt(event)
            self.ingest(event, fault_kind=fault)
        return self.stats