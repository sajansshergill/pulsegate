"""Simulated social-app event generator.

Every event is validated against its contract *before* emit — this is the
producer-side enforcement the logging framework promises. A ``--corrupt-rate``
knob deliberately injects malformed events so the downstream observability /
triage layer (and the test suite) has realistic failures to catch.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
from contracts.registry import ContractRegistry  # noqa: E402

SURFACES = ["feed", "reels", "stories", "messaging", "ads", "notifications"]
EVENT_TYPES = [
    "session_start", "post_create", "like", "follow",
    "message_send", "ad_impression", "ad_click",
]


def _uid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _envelope(event_type: str, user_id: str) -> dict:
    return {
        "event_id": _uid(),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": _now(),
        "user_id": user_id,
        "surface": random.choice(SURFACES),
    }


def generate_event(event_type: str | None = None, user_id: str | None = None) -> dict:
    """Build one well-formed event of the given (or random) type."""
    event_type = event_type or random.choice(EVENT_TYPES)
    user_id = user_id or f"u_{random.randint(1, 5000):05d}"
    e = _envelope(event_type, user_id)

    if event_type == "session_start":
        e |= {
            "session_id": _uid(),
            "device_type": random.choice(["ios", "android", "web"]),
            "app_version": f"{random.randint(3, 9)}.{random.randint(0, 12)}.{random.randint(0, 9)}",
        }
    elif event_type == "post_create":
        e |= {
            "post_id": _uid(),
            "content_type": random.choice(["photo", "video", "text"]),
            "caption_length": random.randint(0, 280),
        }
    elif event_type == "like":
        e |= {"target_id": _uid(), "target_type": random.choice(["post", "comment"])}
    elif event_type == "follow":
        e |= {"followee_id": f"u_{random.randint(1, 5000):05d}"}
    elif event_type == "message_send":
        e |= {"message_id": _uid(), "thread_id": _uid(), "char_count": random.randint(1, 500)}
    elif event_type == "ad_impression":
        e |= {
            "ad_id": _uid(),
            "campaign_id": _uid(),
            "placement": random.choice(["feed", "reels", "stories", "search"]),
        }
    elif event_type == "ad_click":
        e |= {"ad_id": _uid(), "campaign_id": _uid(), "impression_id": _uid()}
    return e


def corrupt(event: dict) -> tuple[dict, str]:
    """Deliberately break a valid event. Returns (event, fault_kind)."""
    kind = random.choice(["drop_field", "wrong_type", "extra_field", "bad_enum"])
    e = dict(event)
    if kind == "drop_field":
        droppable = [k for k in e if k not in ("event_type", "event_version")]
        e.pop(random.choice(droppable))
    elif kind == "wrong_type":
        e["occurred_at"] = 1234567890  # should be an ISO string
    elif kind == "extra_field":
        e["debug_flag"] = True  # additionalProperties:false -> rejected (schema drift)
    elif kind == "bad_enum":
        e["surface"] = "telepathy"
    return e, kind


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate simulated product events (JSONL to stdout).")
    ap.add_argument("-n", "--count", type=int, default=100)
    ap.add_argument("--corrupt-rate", type=float, default=0.0,
                    help="fraction of events to deliberately corrupt (0.0-1.0)")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    registry = ContractRegistry()
    emitted = rejected = 0

    for _ in range(args.count):
        event = generate_event()
        fault = None
        if random.random() < args.corrupt_rate:
            event, fault = corrupt(event)

        result = registry.validate(event)
        if result.ok:
            print(json.dumps(event))
            emitted += 1
        else:
            rejected += 1
            print(json.dumps({
                "_rejected": True,
                "fault_kind": fault,
                "event_type": result.event_type,
                "errors": result.errors,
            }), file=sys.stderr)

    print(f"[generator] emitted={emitted} rejected={rejected}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())