"""End-to-end interleaving tests across the three in-process services."""
import asyncio
import json

import httpx
import pytest

from webapp.storage import (
    P_CONFIRMED,
    P_FAILED,
    P_IN_FLIGHT,
    P_PENDING,
    Storage,
)
from webapp.sync import Pusher
from webapp.config import settings
from conftest import SAMPLE_CSV


async def _upload(wired):
    return await _upload_csv(wired, SAMPLE_CSV)


async def _upload_csv(wired, csv_text):
    r = await wired["web_client"].post(
        "/api/schedule",
        files={"file": ("schedule.csv", csv_text, "text/csv")},
        data={"service_date": "2026-11-16"},
    )
    assert r.status_code == 200, r.text
    return r.json()


CORRECTED_CSV = """trip_id,stop_sequence,stop_code,scheduled_time
T1,1,ST-X,08:00:00
T1,2,ST-Y,08:05:00
T1,3,ST-Z,08:10:00
T2,1,ST-X,09:00:00
T2,2,ST-Y,09:05:00
"""

CORRECTED2_CSV = """trip_id,stop_sequence,stop_code,scheduled_time
T1,1,ST-N1,08:00:00
T1,2,ST-N2,08:05:00
T1,3,ST-N3,08:10:00
T2,1,ST-N1,09:00:00
T2,2,ST-N2,09:05:00
"""


async def _snapshot(gateway, header_ts, entities):
    r = await gateway.post(
        "/admin/snapshot", json={"header_ts": header_ts, "entities": entities}
    )
    assert r.status_code == 200
    return r.json()


def _pred(trip_id, base, n=3):
    return {
        "trip_id": trip_id,
        "stops": [
            {"seq": i, "arrival": base + i * 60, "departure": base + i * 60 + 10}
            for i in range(1, n + 1)
        ],
    }


@pytest.mark.asyncio
async def test_happy_path_poll_then_push_and_reconcile(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100), _pred("T2", 1200, 2)])

    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied" and out["version"] == 1

    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed"

    state = await wired["sign"].get("/state")
    assert state.status_code == 200
    sign_body = state.json()
    # v1 = the uploaded plan itself, v2 = the first snapshot-based state.
    assert sign_body["version"] == 2
    t1 = next(t for t in sign_body["trips"] if t["trip_id"] == "T1")
    assert t1["stops"][0]["arrival_iso"].endswith("Z") and t1["stops"][0]["arrival_time"]

    # Web API exposes the same data plus evidence.
    trips = (await wired["web_client"].get("/api/trips")).json()
    assert {t["trip_id"] for t in trips["trips"]} == {"T1", "T2"}
    ev = (await wired["web_client"].get("/api/evidence/T1")).json()
    assert ev["stops"][0]["arrival_iso"] == t1["stops"][0]["arrival_iso"]
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["in_sync"] is True and sync["stale"] is False
    assert sync["sign_effective_version"] == 2


@pytest.mark.asyncio
async def test_trip_missing_from_new_snapshot_revokes_to_pending(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100), _pred("T2", 1200, 2)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1300)])  # T2 absent
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"
    trips = (await wired["web_client"].get("/api/trips")).json()["trips"]
    t2 = next(t for t in trips if t["trip_id"] == "T2")
    assert all(s["status"] == "PENDING" for s in t2["stops"])
    t1 = next(t for t in trips if t["trip_id"] == "T1")
    assert t1["stops"][0]["arrival_time"] == 1360


@pytest.mark.asyncio
async def test_cancellation_snapshot_and_payload(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000,
                    [{"trip_id": "T1", "cancelled": True}, _pred("T2", 1200, 2)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    sign_body = (await wired["sign"].get("/state")).json()
    t1 = next(t for t in sign_body["trips"] if t["trip_id"] == "T1")
    assert t1["cancelled"] is True
    assert all(s["status"] == "CANCELLED" for s in t1["stops"])


@pytest.mark.asyncio
async def test_fetch_and_parse_failures_keep_state_and_mark_stale(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)

    # Malformed body -> parse error, last-good state retained.
    await wired["gateway"].post("/admin/failure", json={"mode": "MALFORMED"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "parse_error"
    trips = (await wired["web_client"].get("/api/trips")).json()["trips"]
    assert next(t for t in trips if t["trip_id"] == "T1")["stops"][0]["status"] == "PREDICTED"
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["stale"] is True and sync["poll_state"] == "ERROR"

    # HTTP 500 -> fetch error, still retained.
    await wired["gateway"].post("/admin/failure", json={"mode": "HTTP500"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "fetch_error"
    trips = (await wired["web_client"].get("/api/trips")).json()["trips"]
    assert next(t for t in trips if t["trip_id"] == "T1")["stops"][0]["status"] == "PREDICTED"

    # Recovery: a fresh snapshot applies and clears the stale flag.
    await wired["gateway"].post("/admin/failure", json={"mode": "NONE"})
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1400)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["stale"] is False and sync["poll_state"] == "OK"


@pytest.mark.asyncio
async def test_slow_gateway_times_out_and_is_stale(wired):
    await _upload(wired)
    # Simulated network latency beyond the configured HTTP timeout.
    wired["transport"].latency["gateway"] = 5.0
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "fetch_error"
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["stale"] is True
    wired["transport"].latency.pop("gateway", None)


@pytest.mark.asyncio
async def test_late_snapshot_cannot_roll_back(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
    # Late arrival with older header_ts.
    await _snapshot(wired["gateway"], 1500, [_pred("T1", 9999)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "stale_ignored"
    trips = (await wired["web_client"].get("/api/trips")).json()["trips"]
    assert next(t for t in trips if t["trip_id"] == "T1")["stops"][0]["arrival_time"] == 1160
    snaps = wired["storage"].get_snapshots()
    rejected = [s for s in snaps if s["applied"] == 0]
    assert rejected and "stale" in rejected[0]["reject_reason"]

    # Same header_ts again is also ignored (non-increasing).
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 9999)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "stale_ignored"


@pytest.mark.asyncio
async def test_late_old_push_cannot_roll_sign_back(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1400)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    assert (await wired["sign"].get("/state")).json()["version"] == 3

    # Directly forge an old-version push against the sign.
    old = wired["storage"].get_attempt(1)
    r = await wired["sign"].post(
        "/state",
        json={"version": 1, "content_hash": "old", "trips": []},
        headers={"Idempotency-Key": old["idem_key"]},
    )
    assert r.status_code == 409
    assert (await wired["sign"].get("/state")).json()["version"] == 3


@pytest.mark.asyncio
async def test_sign_offline_does_not_block_collection(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)

    wired["transport"].sign_offline = True
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "push_error"
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["sign_reachable"] is False
    # The attempt is FAILED and will be requeued by the background loop.
    attempt = wired["storage"].get_attempt(2)
    assert attempt["status"] == P_FAILED

    # Collection keeps advancing while the sign is down.
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1500)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"
    # New version supersedes the old failed attempt and goes PENDING.
    attempt2 = wired["storage"].get_attempt(3)
    assert attempt2["status"] == P_PENDING

    # Sign recovers: requeue failures then push confirms the newest version.
    wired["transport"].sign_offline = False
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed"
    assert (await wired["sign"].get("/state")).json()["version"] == 3
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["in_sync"] is True


@pytest.mark.asyncio
async def test_lost_receipt_then_reconcile_continues_sync(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    # Make the sign commit but answer 500 (receipt lost after durable write).
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "HTTP500"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "http_error"
    attempt = wired["storage"].get_attempt(2)
    assert attempt["status"] == P_FAILED

    # Sign actually has the data (durable commit happened).
    sign_body = (await wired["sign"].get("/state")).json()
    assert sign_body["version"] == 2

    # Reconciliation observes it and confirms the local attempt by hash.
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "OFF"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        result = await wired["pusher"].reconcile(c)
    assert result["reachable"] is True and result["version"] == 2
    assert wired["storage"].get_attempt(2)["status"] == P_CONFIRMED


@pytest.mark.asyncio
async def test_lost_receipt_hang_then_idempotent_retry(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    # Sign commits the POST then hangs (HANG sleeps after the DB commit);
    # the transport emulates the client read timeout once the app ran.
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "HANG"})
    wired["transport"].hang_after_rules.append(("sign", "POST", "/state"))
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        out = await wired["pusher"].push_once(c)  # times out after commit
    assert out["outcome"] == "push_error"
    assert (await wired["sign"].get("/state")).json()["version"] == 2

    # Retry after the network heals: same idempotency key -> 200 no-op.
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "OFF"})
    wired["transport"].hang_after_rules.clear()
    wired["storage"].requeue_failed()
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed"
    history = (await wired["sign"].get("/admin/history")).json()["history"]
    # Only one durable version row despite two attempts.
    assert [h["version"] for h in history] == [2]


@pytest.mark.asyncio
async def test_restart_requeues_inflight_and_catches_up(wired, tmp_path):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    assert (await wired["sign"].get("/state")).json()["version"] == 2

    # A second version is produced but its push crashes mid-flight.
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1500)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
    claimed = wired["storage"].claim_next_push()
    assert claimed["status"] == P_IN_FLIGHT and claimed["version"] == 3
    db_path = wired["storage"].db_path

    # "Restart": a brand-new Storage on the same database file.
    restarted = Storage(db_path)
    restarted.reset_inflight_on_startup()
    assert restarted.get_attempt(3)["status"] == P_FAILED

    pusher2 = Pusher(restarted, settings, transport=wired["transport"])
    # Reconcile first: the sign still serves v2, so nothing is wrongly
    # confirmed; then a normal idempotent push of v3 catches up.
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        rec = await pusher2.reconcile(c)
    assert rec["version"] == 2
    assert restarted.get_attempt(3)["status"] == P_FAILED
    restarted.requeue_failed()
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await pusher2.push_once(c)
    assert out["outcome"] == "confirmed"
    assert (await wired["sign"].get("/state")).json()["version"] == 3
    restarted.close()


@pytest.mark.asyncio
async def test_concurrent_pushers_only_one_in_flight(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)

    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        results = await asyncio.gather(
            wired["pusher"].push_once(c),
            wired["pusher"].push_once(c),
        )
    outcomes = sorted(r["outcome"] for r in results)
    assert outcomes == ["confirmed", "nothing_to_push"]
    assert (await wired["sign"].get("/state")).json()["version"] == 2


@pytest.mark.asyncio
async def test_manual_push_requeues_failed_attempt(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        wired["transport"].sign_offline = True
        out = await wired["pusher"].push_once(c)  # fails -> FAILED
        assert out["outcome"] == "push_error"
        wired["transport"].sign_offline = False
        # Next manual push requeues the FAILED attempt and confirms.
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed"
    assert (await wired["sign"].get("/state")).json()["version"] == 2


@pytest.mark.asyncio
async def test_trips_all_pending_before_any_snapshot(wired):
    await _upload(wired)
    j = (await wired["web_client"].get("/api/trips")).json()
    assert j["realtime_pending"] is True
    assert {t["trip_id"] for t in j["trips"]} == {"T1", "T2"}
    assert all(s["status"] == "PENDING" for t in j["trips"] for s in t["stops"])


@pytest.mark.asyncio
async def test_unknown_trip_ignored_count(wired):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [
        _pred("T1", 1100), {"trip_id": "GHOST", "stops": []}
    ])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"
    snap = wired["storage"].latest_applied_snapshot()
    assert snap["ignored_count"] == 1


# ------------------------------------------------------- plan correction
@pytest.mark.asyncio
async def test_plan_correction_syncs_to_sign_without_new_snapshot(wired):
    """The reported bug: after a plan correction the sign must receive the
    new plan's full PENDING detail without waiting for a new snapshot."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100), _pred("T2", 1200, 2)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    sign_body = (await wired["sign"].get("/state")).json()
    assert sign_body["version"] == 2
    t1 = next(t for t in sign_body["trips"] if t["trip_id"] == "T1")
    assert t1["stops"][0]["stop_code"] == "ST-A"
    assert t1["stops"][0]["status"] == "PREDICTED"

    # Correct the day's plan: new stop codes. Locally all PENDING.
    resp = await _upload_csv(wired, CORRECTED_CSV)
    assert resp["desired_version"] == 3
    trips = (await wired["web_client"].get("/api/trips")).json()
    assert trips["realtime_pending"] is True
    t1 = next(t for t in trips["trips"] if t["trip_id"] == "T1")
    assert [s["stop_code"] for s in t1["stops"]] == ["ST-X", "ST-Y", "ST-Z"]
    assert all(s["status"] == "PENDING" for s in t1["stops"])

    # No new realtime snapshot needed: one push syncs the corrected plan.
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed" and out["version"] == 3
    sign_body = (await wired["sign"].get("/state")).json()
    assert sign_body["version"] == 3
    assert sign_body["header_ts"] is None and sign_body["rt_version"] is None
    t1 = next(t for t in sign_body["trips"] if t["trip_id"] == "T1")
    assert [s["stop_code"] for s in t1["stops"]] == ["ST-X", "ST-Y", "ST-Z"]
    assert all(s["status"] == "PENDING" for s in t1["stops"])

    # The plan push must not fake a collection success...
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["poll_state"] == "NO_DATA"
    assert sync["last_success_at"] is None
    assert sync["realtime_pending"] is True
    assert sync["in_sync"] is True

    # ...and must not block a later valid snapshot.
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 2100), _pred("T2", 2200, 2)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["pusher"].push_once(c)
    sign_body = (await wired["sign"].get("/state")).json()
    t1 = next(t for t in sign_body["trips"] if t["trip_id"] == "T1")
    assert t1["stops"][0]["stop_code"] == "ST-X"  # corrected plan kept
    assert t1["stops"][0]["status"] == "PREDICTED"


@pytest.mark.asyncio
async def test_inflight_old_push_cannot_roll_back_corrected_plan(wired):
    """Exact bug interleaving: plan corrected while an old push is in
    flight; the old push lands old stop codes, then the new plan overtakes
    and a restart has nothing left to catch up."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100), _pred("T2", 1200, 2)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
    # Old push claimed (payload frozen: old stop codes + predictions)...
    claimed_old = wired["storage"].claim_next_push()
    old_payload = json.loads(claimed_old["payload_json"])
    # ...the plan is corrected while the old push is still in flight...
    await _upload_csv(wired, CORRECTED_CSV)
    # ...and only then does the old push land on the sign.
    r = await wired["sign"].post(
        "/state", json=old_payload,
        headers={"Idempotency-Key": old_payload["idempotency_key"]},
    )
    assert r.status_code == 201
    stale = (await wired["sign"].get("/state")).json()
    assert stale["trips"][0]["stops"][0]["stop_code"] == "ST-A"  # old, stale

    # The corrected plan is synced right after, without a new snapshot.
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed" and out["version"] == 3
    body = (await wired["sign"].get("/state")).json()
    assert body["trips"][0]["stops"][0]["stop_code"] == "ST-X"
    assert all(
        s["status"] == "PENDING" for t in body["trips"] for s in t["stops"]
    )

    # A restart at this point has nothing left to catch up: fully in sync.
    restarted = Storage(wired["storage"].db_path)
    restarted.reset_inflight_on_startup()
    pusher2 = Pusher(restarted, settings, transport=wired["transport"])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await pusher2.push_once(c)
    assert out["outcome"] == "nothing_to_push"
    restarted.close()


@pytest.mark.asyncio
async def test_consecutive_corrections_old_receipt_cannot_confirm_new(wired):
    """With consecutive corrections, an old receipt must not confirm the
    new plan and a replayed old push must not roll the sign back."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    assert (await wired["sign"].get("/state")).json()["version"] == 2

    # Correction B is claimed (payload frozen) but its send is in flight.
    await _upload_csv(wired, CORRECTED_CSV)
    claimed_b = wired["storage"].claim_next_push()
    assert claimed_b["version"] == 3 and claimed_b["status"] == P_IN_FLIGHT
    payload_b = json.loads(claimed_b["payload_json"])
    assert payload_b["trips"][0]["stops"][0]["stop_code"] == "ST-X"

    # Correction C replaces B before B's send finishes.
    await _upload_csv(wired, CORRECTED2_CSV)
    assert wired["storage"].get_attempt(3) is None  # B's attempt is gone
    assert wired["storage"].get_attempt(4)["status"] == P_PENDING

    # B's late receipt arrives now: it must NOT confirm C's attempt.
    assert wired["storage"].mark_push_confirmed(claimed_b["id"], 201) is False
    assert wired["storage"].get_attempt(4)["status"] == P_PENDING

    # B's late push lands on the sign (v3 > current v2): accepted...
    r = await wired["sign"].post(
        "/state", json=payload_b,
        headers={"Idempotency-Key": payload_b["idempotency_key"]},
    )
    assert r.status_code == 201
    # ...but the next push syncs C (v4); B can never roll C back.
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed" and out["version"] == 4
    body = (await wired["sign"].get("/state")).json()
    assert body["version"] == 4
    assert body["trips"][0]["stops"][0]["stop_code"] == "ST-N1"

    # A replay of B after C took effect is rejected by the sign (409).
    r = await wired["sign"].post(
        "/state", json=payload_b,
        headers={"Idempotency-Key": payload_b["idempotency_key"]},
    )
    assert r.status_code == 409
    assert (await wired["sign"].get("/state")).json()["version"] == 4


@pytest.mark.asyncio
async def test_claimed_payload_frozen_with_same_basis(wired):
    """Each send's content, hash and receipt belong to the same plan +
    realtime basis: the payload is frozen atomically at claim time."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
    claimed = wired["storage"].claim_next_push()
    payload = json.loads(claimed["payload_json"])
    # Hash stored on the attempt matches the frozen payload exactly, and
    # the content carries its realtime basis explicitly.
    assert claimed["content_hash"] == payload["content_hash"]
    assert payload["version"] == claimed["version"] == 2
    assert payload["header_ts"] == 1000 and payload["rt_version"] == 1
    assert payload["trips"][0]["stops"][0]["stop_code"] == "ST-A"

    # Plan replaced while that push is in flight: the next claim freezes a
    # different, self-consistent payload (new plan, no realtime basis).
    await _upload_csv(wired, CORRECTED_CSV)
    claimed_new = wired["storage"].claim_next_push()
    payload_new = json.loads(claimed_new["payload_json"])
    assert payload_new["version"] == 3
    assert payload_new["header_ts"] is None and payload_new["rt_version"] is None
    assert payload_new["trips"][0]["stops"][0]["stop_code"] == "ST-X"
    assert payload_new["content_hash"] != payload["content_hash"]
    assert claimed_new["content_hash"] == payload_new["content_hash"]


@pytest.mark.asyncio
async def test_restart_between_upload_and_push_resumes_correction(wired):
    """App exits between upload and send: restart must resume the pending
    correction instead of losing it."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    assert (await wired["sign"].get("/state")).json()["version"] == 2

    # Correct the plan, then "crash" before the pusher picks it up.
    await _upload_csv(wired, CORRECTED_CSV)
    restarted = Storage(wired["storage"].db_path)
    restarted.reset_inflight_on_startup()
    # The correction survived the restart as a PENDING attempt.
    pending = restarted.get_push_attempts()
    assert len(pending) == 1 and pending[0]["status"] == P_PENDING
    assert pending[0]["version"] == 3

    pusher2 = Pusher(restarted, settings, transport=wired["transport"])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await pusher2.push_once(c)
    assert out["outcome"] == "confirmed"
    body = (await wired["sign"].get("/state")).json()
    assert body["version"] == 3
    t1 = next(t for t in body["trips"] if t["trip_id"] == "T1")
    assert [s["stop_code"] for s in t1["stops"]] == ["ST-X", "ST-Y", "ST-Z"]
    assert all(s["status"] == "PENDING" for s in t1["stops"])
    restarted.close()


@pytest.mark.asyncio
async def test_restart_with_lost_receipt_reconciles_correction(wired):
    """Sign persisted the correction but the receipt was lost, then the
    app exits before confirming: restart must verify by content hash."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)

    # Correct the plan; the sign commits v3 but the receipt is lost.
    await _upload_csv(wired, CORRECTED_CSV)
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "HTTP500"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "http_error"
    assert (await wired["sign"].get("/state")).json()["version"] == 3

    # "Restart": reconciliation confirms the attempt by content hash.
    restarted = Storage(wired["storage"].db_path)
    restarted.reset_inflight_on_startup()
    assert restarted.get_attempt(3)["status"] == P_FAILED
    pusher2 = Pusher(restarted, settings, transport=wired["transport"])
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "OFF"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        rec = await pusher2.reconcile(c)
    assert rec["version"] == 3
    assert restarted.get_attempt(3)["status"] == P_CONFIRMED
    summary = restarted.get_sync_summary()
    assert summary["sign_observed_version"] == 3 == summary["desired_version"]
    restarted.close()


@pytest.mark.asyncio
async def test_sign_offline_upload_not_blocked_and_pending_visible(wired):
    """Sign offline must not block upload/collection; the UI distinguishes
    pending-sync content from what the sign actually serves."""
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)

    wired["transport"].sign_offline = True
    # Upload succeeds even though the sign is offline.
    resp = await _upload_csv(wired, CORRECTED_CSV)
    assert resp["desired_version"] == 3
    # Push fails but does not block; UI shows 待同步 vs 实际生效.
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "push_error"
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["local_latest"] == 3
    assert sync["sign_effective_version"] == 2
    assert sync["in_sync"] is False
    # Collection is not blocked by the offline sign.
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 2100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"

    # Sign recovers: catches up straight to the newest version (corrected
    # plan stop codes + new snapshot predictions).
    wired["transport"].sign_offline = False
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed" and out["version"] == 4
    body = (await wired["sign"].get("/state")).json()
    t1 = next(t for t in body["trips"] if t["trip_id"] == "T1")
    assert t1["stops"][0]["stop_code"] == "ST-X"
    assert t1["stops"][0]["status"] == "PREDICTED"
