"""Bronze layer writer.

Consumes ``events.raw`` and ``events.deadletter`` off the bus and lands them in
DuckDB. Bronze is append-only and near-raw: the full payload is preserved as
JSON, with a few envelope columns lifted out for queryability. Ingestion is
idempotent — re-consuming the same offsets is a no-op — so a restart never
double-counts. Deduplication of business-level duplicates is a silver concern,
not bronze's.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from ingest.producer import DEAD_LETTER_TOPIC, EVENTS_TOPIC
from ingest.transport import MessageBus


class BronzeWriter:
    def __init__(self, db_path: str = "warehouse/bronze.duckdb"):
        self.con = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bronze_events (
                topic          VARCHAR,
                kafka_offset   BIGINT,
                event_id       VARCHAR,
                event_type     VARCHAR,
                event_version  INTEGER,
                user_id        VARCHAR,
                surface        VARCHAR,
                occurred_at    VARCHAR,
                payload        VARCHAR,
                ingested_at    TIMESTAMP,
                PRIMARY KEY (topic, kafka_offset)
            );
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter (
                topic          VARCHAR,
                kafka_offset   BIGINT,
                event_type     VARCHAR,
                fault_kind     VARCHAR,
                errors         VARCHAR,
                payload        VARCHAR,
                ingested_at    TIMESTAMP,
                PRIMARY KEY (topic, kafka_offset)
            );
        """)

    def consume(
        self,
        bus: MessageBus,
        events_topic: str = EVENTS_TOPIC,
        dead_letter_topic: str = DEAD_LETTER_TOPIC,
        offset: int = 0,
    ) -> dict[str, int]:
        now = datetime.now(timezone.utc)
        before = self.counts()

        # INSERT OR IGNORE + PRIMARY KEY(topic, offset) makes replays no-ops, so
        # written counts are measured as table deltas rather than from rowcount
        # (which DuckDB does not report reliably for ignored conflicts).
        for m in bus.read(events_topic, offset):
            e = m.value
            self.con.execute(
                """INSERT OR IGNORE INTO bronze_events VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [m.topic, m.offset, e.get("event_id"), e.get("event_type"),
                 e.get("event_version"), e.get("user_id"), e.get("surface"),
                 e.get("occurred_at"), json.dumps(e), now],
            )

        for m in bus.read(dead_letter_topic, offset):
            d = m.value
            self.con.execute(
                """INSERT OR IGNORE INTO dead_letter VALUES (?,?,?,?,?,?,?)""",
                [m.topic, m.offset, d.get("event_type"), d.get("fault_kind"),
                 json.dumps(d.get("errors")), json.dumps(d.get("payload")), now],
            )

        after = self.counts()
        return {k: after[k] - before[k] for k in after}

    def counts(self) -> dict[str, int]:
        return {
            "bronze_events": self.con.execute("SELECT count(*) FROM bronze_events").fetchone()[0],
            "dead_letter": self.con.execute("SELECT count(*) FROM dead_letter").fetchone()[0],
        }

    def volume_by_type(self) -> list[tuple[str, int]]:
        return self.con.execute(
            "SELECT event_type, count(*) c FROM bronze_events GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()

    def close(self) -> None:
        self.con.close()