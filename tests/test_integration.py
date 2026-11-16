"""End-to-end interleaving tests across the three in-process services."""
import asyncio

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
    r = await wired["web_client"].post(
        "/api/schedule",
        files={"file": ("schedule.csv", SAMPLE_CSV, "text/csv")},
        data={"service_date": "2026-11-16"},
    )
    assert r.status_code == 200, r.text
    return r.json()


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
    assert sign_body["version"] == 1
    t1 = next(t for t in sign_body["trips"] if t["trip_id"] == "T1")
    assert t1["stops"][0]["arrival_iso"].endswith("Z") and t1["stops"][0]["arrival_time"]

    # Web API exposes the same data plus evidence.
    trips = (await wired["web_client"].get("/api/trips")).json()
    assert {t["trip_id"] for t in trips["trips"]} == {"T1", "T2"}
    ev = (await wired["web_client"].get("/api/evidence/T1")).json()
    assert ev["stops"][0]["arrival_iso"] == t1["stops"][0]["arrival_iso"]
    sync = (await wired["web_client"].get("/api/sync")).json()
    assert sync["in_sync"] is True and sync["stale"] is False
    assert sync["sign_effective_version"] == 1


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
    assert (await wired["sign"].get("/state")).json()["version"] == 2

    # Directly forge an old-version push against the sign.
    old = wired["storage"].get_attempt(1)
    r = await wired["sign"].post(
        "/state",
        json={"version": 1, "content_hash": "old", "trips": []},
        headers={"Idempotency-Key": old["idem_key"]},
    )
    assert r.status_code == 409
    assert (await wired["sign"].get("/state")).json()["version"] == 2


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
    attempt = wired["storage"].get_attempt(1)
    assert attempt["status"] == P_FAILED

    # Collection keeps advancing while the sign is down.
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1500)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["poller"].poll_once(c)
    assert out["outcome"] == "applied"
    # New version supersedes the old failed attempt and goes PENDING.
    attempt2 = wired["storage"].get_attempt(2)
    assert attempt2["status"] == P_PENDING

    # Sign recovers: requeue failures then push confirms the newest version.
    wired["transport"].sign_offline = False
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed"
    assert (await wired["sign"].get("/state")).json()["version"] == 2
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
    attempt = wired["storage"].get_attempt(1)
    assert attempt["status"] == P_FAILED

    # Sign actually has the data (durable commit happened).
    sign_body = (await wired["sign"].get("/state")).json()
    assert sign_body["version"] == 1

    # Reconciliation observes it and confirms the local attempt by hash.
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "OFF"})
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        result = await wired["pusher"].reconcile(c)
    assert result["reachable"] is True and result["version"] == 1
    assert wired["storage"].get_attempt(1)["status"] == P_CONFIRMED


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
    assert (await wired["sign"].get("/state")).json()["version"] == 1

    # Retry after the network heals: same idempotency key -> 200 no-op.
    await wired["sign"].post("/admin/lose-receipt", json={"mode": "OFF"})
    wired["transport"].hang_after_rules.clear()
    wired["storage"].requeue_failed()
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await wired["pusher"].push_once(c)
    assert out["outcome"] == "confirmed"
    history = (await wired["sign"].get("/admin/history")).json()["history"]
    # Only one durable version row despite two attempts.
    assert [h["version"] for h in history] == [1]


@pytest.mark.asyncio
async def test_restart_requeues_inflight_and_catches_up(wired, tmp_path):
    await _upload(wired)
    await _snapshot(wired["gateway"], 1000, [_pred("T1", 1100)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
        await wired["pusher"].push_once(c)
    assert (await wired["sign"].get("/state")).json()["version"] == 1

    # A second version is produced but its push crashes mid-flight.
    await _snapshot(wired["gateway"], 2000, [_pred("T1", 1500)])
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        await wired["poller"].poll_once(c)
    claimed = wired["storage"].claim_next_push()
    assert claimed["status"] == P_IN_FLIGHT and claimed["version"] == 2
    db_path = wired["storage"].db_path

    # "Restart": a brand-new Storage on the same database file.
    restarted = Storage(db_path)
    restarted.reset_inflight_on_startup()
    assert restarted.get_attempt(2)["status"] == P_FAILED

    pusher2 = Pusher(restarted, settings, transport=wired["transport"])
    # Reconcile first: the sign still serves v1, so nothing is wrongly
    # confirmed; then a normal idempotent push of v2 catches up.
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        rec = await pusher2.reconcile(c)
    assert rec["version"] == 1
    assert restarted.get_attempt(2)["status"] == P_FAILED
    restarted.requeue_failed()
    async with httpx.AsyncClient(transport=wired["transport"]) as c:
        out = await pusher2.push_once(c)
    assert out["outcome"] == "confirmed"
    assert (await wired["sign"].get("/state")).json()["version"] == 2
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
    assert (await wired["sign"].get("/state")).json()["version"] == 1


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
    assert (await wired["sign"].get("/state")).json()["version"] == 1


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
