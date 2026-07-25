"""Triage engine.

Runs the detectors against bronze and turns raw findings into structured triage
records — the artifact a human (or the phase-4 AI triager) actually works from.
Each record carries severity, a suggested owner, and the evidence that justifies
it. Records are keyed by a content fingerprint so re-running the engine on an
unchanged situation doesn't spam duplicates.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone

from quality.baselines import Baseline, collect_snapshot
from quality.detectors import DEFAULT_DETECTORS, Detector, Finding

# Which team owns which class of problem. Logging efficacy is a shared concern,
# so a finding is useless without a clear "whose is this".
OWNER_MAP = {
    "volume_anomaly": "ingest-oncall",
    "null_rate": "producer-team",
    "duplicate_event": "ingest-oncall",
    "dead_letter_rate": "producer-team",
    "schema_drift": "producer-team",
    "referential_integrity": "analytics-eng",
}
SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}


@dataclass
class TriageRecord:
    fingerprint: str
    detector: str
    signal: str
    severity: str
    event_type: str | None
    detail: str
    suggested_owner: str
    evidence: dict
    first_seen: datetime

    @staticmethod
    def _fingerprint(f: Finding) -> str:
        raw = f"{f.detector}|{f.signal}|{f.event_type}"
        return hashlib.sha1(raw.encode()).hexdigest()[:12]

    @classmethod
    def from_finding(cls, f: Finding, now: datetime) -> "TriageRecord":
        return cls(
            fingerprint=cls._fingerprint(f),
            detector=f.detector,
            signal=f.signal,
            severity=f.severity,
            event_type=f.event_type,
            detail=f.detail,
            suggested_owner=OWNER_MAP.get(f.detector, "data-platform"),
            evidence=f.evidence,
            first_seen=now,
        )


class TriageEngine:
    def __init__(self, con, detectors: list[Detector] | None = None):
        self.con = con
        self.detectors = detectors if detectors is not None else DEFAULT_DETECTORS
        self._init_schema()

    def _init_schema(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS triage_records (
                fingerprint     VARCHAR PRIMARY KEY,
                detector        VARCHAR,
                signal          VARCHAR,
                severity        VARCHAR,
                event_type      VARCHAR,
                detail          VARCHAR,
                suggested_owner VARCHAR,
                evidence        VARCHAR,
                first_seen      TIMESTAMP,
                last_seen       TIMESTAMP,
                seen_count      INTEGER
            );
        """)

    def run(self, baseline: Baseline, extra_detectors: list[Detector] | None = None) -> list[TriageRecord]:
        snap = collect_snapshot(self.con)
        detectors = self.detectors + (extra_detectors or [])
        now = datetime.now(timezone.utc)

        findings: list[Finding] = []
        for d in detectors:
            findings.extend(d.run(snap, baseline))

        records = [TriageRecord.from_finding(f, now) for f in findings]
        for r in records:
            # Upsert: a recurring finding bumps last_seen / seen_count instead of
            # creating a second record for the same underlying problem.
            self.con.execute("""
                INSERT INTO triage_records VALUES (?,?,?,?,?,?,?,?,?,?,1)
                ON CONFLICT (fingerprint) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    seen_count = triage_records.seen_count + 1,
                    severity = excluded.severity,
                    detail = excluded.detail,
                    evidence = excluded.evidence;
            """, [r.fingerprint, r.detector, r.signal, r.severity, r.event_type,
                  r.detail, r.suggested_owner, json.dumps(r.evidence), r.first_seen, now])

        records.sort(key=lambda r: SEVERITY_RANK[r.severity], reverse=True)
        return records

    def open_records(self) -> list[tuple]:
        return self.con.execute(
            "SELECT severity, detector, event_type, detail, suggested_owner, seen_count "
            "FROM triage_records "
            "ORDER BY CASE severity WHEN 'critical' THEN 2 WHEN 'warning' THEN 1 ELSE 0 END DESC"
        ).fetchall()