"""Build the full per-stop detail payload pushed to the sign service.

The payload is the complete desired state of the sign for the service
day, so the sign never has to merge anything: a newer push fully
replaces an older one. Its canonical JSON hash is used for
reconciliation after lost receipts and restarts.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from .storage import Storage

# Per-stop statuses rendered on the sign.
ST_PREDICTED = "PREDICTED"  # 有绝对预计时刻
ST_PENDING = "PENDING"      # 待定（无实时依据 / 快照中缺席）
ST_SKIPPED = "SKIPPED"      # 该站甩站
ST_CANCELLED = "CANCELLED"  # 整班取消


def epoch_to_iso(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def derive_stop_status(row: dict) -> str:
    if row["cancelled"]:
        return ST_CANCELLED
    if (row.get("sched_rel") or "") == "SKIPPED":
        return ST_SKIPPED
    if row.get("arrival_time") is not None or row.get("departure_time") is not None:
        return ST_PREDICTED
    return ST_PENDING


def build_payload(
    storage: Storage, version: int, idem_key: str | None = None
) -> dict:
    """Full desired sign state for desired-state ``version``.

    The content explicitly carries its realtime basis (``rt_version`` /
    ``header_ts`` of the applied snapshot, both ``None`` right after a plan
    upload) so the sent content, its hash and the sign's receipt always
    describe the same plan + realtime basis.
    """
    rows = storage.get_trip_rows()
    trips: dict[str, dict] = {}
    for row in rows:
        trip = trips.setdefault(
            row["trip_id"],
            {"trip_id": row["trip_id"], "cancelled": bool(row["cancelled"]), "stops": []},
        )
        if row["cancelled"]:
            trip["cancelled"] = True
        trip["stops"].append(
            {
                "stop_sequence": row["stop_sequence"],
                "stop_code": row["stop_code"],
                "scheduled_time": row["scheduled_time"],
                "status": derive_stop_status(row),
                "arrival_time": row["arrival_time"],
                "departure_time": row["departure_time"],
                "arrival_iso": epoch_to_iso(row["arrival_time"]),
                "departure_iso": epoch_to_iso(row["departure_time"]),
                "uncertainty": row["uncertainty"],
                "realtime_version": row["rt_version"],
                "header_ts": row["header_ts"],
            }
        )
    trip_list = [trips[k] for k in sorted(trips)]
    for t in trip_list:
        t["stops"].sort(key=lambda s: s["stop_sequence"])

    snap = storage.latest_applied_snapshot()
    if idem_key is None:
        attempt = storage.get_attempt(version)
        idem_key = attempt["idem_key"] if attempt else f"v{version}"
    # Keep the payload deterministic: no wall-clock fields, otherwise two
    # pushes of the same state would hash differently and break idempotency.
    payload = {
        "version": version,
        "idempotency_key": idem_key,
        "rt_version": snap["version"] if snap else None,
        "header_ts": snap["header_ts"] if snap else None,
        "service_date": storage.get_service_date(),
        "trips": trip_list,
    }
    payload["content_hash"] = content_hash_of(payload)
    return payload


def content_hash_of(payload: dict) -> str:
    """Stable hash over the payload content excluding the hash itself."""
    canonical = {k: v for k, v in payload.items() if k != "content_hash"}
    blob = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()
