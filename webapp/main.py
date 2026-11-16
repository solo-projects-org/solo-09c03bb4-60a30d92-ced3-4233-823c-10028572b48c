"""FastAPI web application: upload, per-stop evidence, sync status."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse

from .config import settings
from .payload import build_payload
from .schedule_io import ScheduleError, parse_schedule_csv
from .storage import Storage
from .sync import Poller, Pusher

_STORAGE: Storage | None = None
_POLLER: Poller | None = None
_PUSHER: Pusher | None = None


def get_storage() -> Storage:
    assert _STORAGE is not None
    return _STORAGE


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _STORAGE, _POLLER, _PUSHER
    Path(os.path.dirname(settings.db_path) or ".").mkdir(parents=True, exist_ok=True)
    _STORAGE = Storage(settings.db_path)
    # Crash recovery: an attempt stuck IN_FLIGHT after a restart is requeued;
    # the idempotency key + sign-side version guard prevent duplicate effects.
    _STORAGE.reset_inflight_on_startup()
    _POLLER = Poller(_STORAGE, settings)
    _PUSHER = Pusher(_STORAGE, settings)
    if settings.background_workers:
        _POLLER.start()
        _PUSHER.start()
    try:
        yield
    finally:
        if settings.background_workers:
            await _POLLER.stop()
            await _PUSHER.stop()
        _STORAGE.close()


app = FastAPI(title="公交逐站预计到站平台", lifespan=lifespan)


# ----------------------------------------------------------------- upload
@app.post("/api/schedule")
async def upload_schedule(
    file: UploadFile = File(...),
    service_date: str = Form(default=""),
):
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw.decode("gb18030")
        except UnicodeDecodeError as exc:
            raise HTTPException(400, "CSV 编码仅支持 UTF-8/GB18030") from exc
    date = service_date.strip() or None
    try:
        inferred_date, rows = parse_schedule_csv(text, date)
    except ScheduleError as exc:
        raise HTTPException(400, str(exc)) from exc
    count = get_storage().replace_schedule(inferred_date, rows)
    return {
        "ok": True,
        "service_date": inferred_date,
        "row_count": count,
        "trip_count": len({r["trip_id"] for r in rows}),
    }


# ------------------------------------------------------------------ data
@app.get("/api/trips")
async def trips():
    s = get_storage()
    if s.get_service_date() is None:
        return {"service_date": None, "trips": []}
    latest = s.latest_applied_snapshot()
    if latest is None:
        # No successful snapshot yet: every scheduled stop is PENDING.
        payload = build_payload(s, 0)
        return {
            "service_date": s.get_service_date(),
            "trips": payload["trips"],
            "realtime_pending": True,
        }
    payload = build_payload(s, latest["version"])
    return {
        "service_date": s.get_service_date(),
        "trips": payload["trips"],
        "realtime_pending": False,
    }


@app.get("/api/evidence/{trip_id}")
async def evidence(trip_id: str):
    """Raw join evidence for one trip, including the GTFS-RT basis per stop."""
    s = get_storage()
    rows = [r for r in s.get_trip_rows() if r["trip_id"] == trip_id]
    if not rows:
        raise HTTPException(404, f"未知班次 {trip_id}")
    return {
        "trip_id": trip_id,
        "cancelled": bool(rows[0]["cancelled"]),
        "rt_version": rows[0]["rt_version"],
        "header_ts": rows[0]["header_ts"],
        "stops": [
            {
                "stop_sequence": r["stop_sequence"],
                "stop_code": r["stop_code"],
                "scheduled_time": r["scheduled_time"],
                "arrival_time": r["arrival_time"],
                "departure_time": r["departure_time"],
                "arrival_iso": _iso(r["arrival_time"]),
                "departure_iso": _iso(r["departure_time"]),
                "uncertainty": r["uncertainty"],
                "sched_rel": r["sched_rel"],
            }
            for r in rows
        ],
    }


def _iso(ts):
    from datetime import datetime, timezone

    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# ------------------------------------------------------------------- sync
@app.get("/api/sync")
async def sync_status():
    s = get_storage()
    summary = s.get_sync_summary()
    latest = s.latest_snapshot()
    # Stale: last successful snapshot is older than a failed poll attempt.
    summary["stale"] = (
        summary["poll_state"] == "ERROR"
        and summary["last_error_at"] is not None
        and (
            summary["last_success_at"] is None
            or summary["last_error_at"] > summary["last_success_at"]
        )
    )
    summary["local_latest"] = summary["latest_version"]
    summary["sign_effective_version"] = summary["sign_observed_version"]
    summary["in_sync"] = (
        summary["latest_version"] is not None
        and summary["sign_observed_version"] == summary["latest_version"]
        and summary["pending_push_count"] == 0
    )
    summary["last_snapshot"] = dict(latest) if latest else None
    summary["push_attempts"] = s.get_push_attempts()
    summary["snapshots"] = s.get_snapshots()
    return summary


@app.post("/api/poll-now")
async def poll_now():
    if _POLLER is None:
        raise HTTPException(503, "轮询器未初始化")
    async with httpx.AsyncClient() as client:
        result = await _POLLER.poll_once(client)
    return result


@app.post("/api/reconcile-now")
async def reconcile_now():
    if _PUSHER is None:
        raise HTTPException(503, "推送器未初始化")
    async with httpx.AsyncClient() as client:
        result = await _PUSHER.reconcile(client)
    return result


@app.post("/api/push-now")
async def push_now():
    if _PUSHER is None:
        raise HTTPException(503, "推送器未初始化")
    async with httpx.AsyncClient() as client:
        result = await _PUSHER.push_once(client)
    return result


# ------------------------------------------------------------------ page
@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML


INDEX_HTML = (Path(__file__).parent / "static" / "index.html").read_text(
    encoding="utf-8"
)
