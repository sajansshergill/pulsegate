"""Baseline and snapshots.

A ``Snapshot`` is the aggregate shape of the data right now (volume per type,
null rates, duplicate ids, dead-letter mix). A ``Baseline`` is the same shape
captured from a known-good reference period. Detectors compare the two - the
baseline is what makes "this dropped" or "this spiked" a definable claim rather
than a gut feeling.

Both are computed straight from DuckDB bronze/dead_letter tables, so they
reflect exactly what landed, not what we hoped would land.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

# Envelope columns lifted into bronze that we monitor for null rate.
MONITORED_COLUMNS = ["event_id", "user_id", "surface", "occurred_at"]


@dataclass
class Snapshot:
    total: int
    volume_by_type: dict[str, int]
    null_rates: dict[str, dict[str, float]]    # event_type -> column -> null fraction
    duplicate_event_ids: int
    dead_letter_total: int
    dead_letter_by_fault: dict[str, int]
    
    @property
    def volume_share(self) -> dict[str, float]:
        return ({t: c / self.total for t, c in self.volume_by_type.items()}
                if self.total else {})
        
    @property
    def dead_letter_rate(self) -> float:
        denom = self.total + self.dead_letter_total
        return self.dead_letter_total / denom if denom else 0.0


@dataclass
class Baseline:
    volume_share: dict[str, float]
    null_rates: dict[str, dict[str, float]]
    dead_letter_rate: float
    sample_size: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, s: str) -> "Baseline":
        return cls(**json.loads(s))


def _count(con, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def collect_snapshot(con) -> Snapshot:
    total = _count(con, "bronze_events")
    volume_by_type = dict(con.execute(
        "SELECT event_type, count(*) FROM bronze_events GROUP BY 1"
    ).fetchall())

    null_rates: dict[str, dict[str, float]] = {}
    for et, cnt in volume_by_type.items():
        if not cnt:
            continue
        cols = ", ".join(
            f"sum(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)::DOUBLE / {cnt}"
            for c in MONITORED_COLUMNS
        )
        row = con.execute(
            f"SELECT {cols} FROM bronze_events WHERE event_type = ?", [et]
        ).fetchone()
        null_rates[et] = dict(zip(MONITORED_COLUMNS, row))

    dup = con.execute("""
        SELECT COALESCE(sum(c - 1), 0)
        FROM (
            SELECT event_id, count(*) AS c
            FROM bronze_events
            WHERE event_id IS NOT NULL
            GROUP BY 1
            HAVING count(*) > 1
        )
    """).fetchone()[0]
    dl_total = _count(con, "dead_letter")
    dl_by_fault = dict(con.execute(
        "SELECT coalesce(fault_kind, 'unknown'), count(*) FROM dead_letter GROUP BY 1"
    ).fetchall())

    return Snapshot(
        total=total,
        volume_by_type=volume_by_type,
        null_rates=null_rates,
        duplicate_event_ids=dup,
        dead_letter_total=dl_total,
        dead_letter_by_fault=dl_by_fault,
    )


def compute_snapshot(con) -> Snapshot:
    return collect_snapshot(con)


def compute_baseline(con) -> Baseline:
    snap = collect_snapshot(con)
    return Baseline(
        volume_share=snap.volume_share,
        null_rates=snap.null_rates,
        dead_letter_rate=snap.dead_letter_rate,
        sample_size=snap.total,
    )