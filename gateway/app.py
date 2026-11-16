"""Mock GTFS-RT gateway.

Serves a complete FeedMessage snapshot at ``GET /gtfs-rt`` and exposes
test-control endpoints so scenarios (slow responses, connection drops,
malformed bodies, stale timestamps, cancellations, missing trips) can be
driven from the browser/curl without rebuilding anything.

State is intentionally in-memory: the gateway is a test double.
"""
from __future__ import annotations

import asyncio
import time

from fastapi import Body, FastAPI
from google.transit import gtfs_realtime_pb2 as pb

app = FastAPI(title="模拟 GTFS-RT 网关", root_path="")

STATE: dict = {
    "delay": 0.0,
    "failure": "NONE",  # NONE | SLOW | DROP | MALFORMED | EMPTY | HTTP500
    "next_header_ts": int(time.time()),
    "entities": [],     # list of dicts (see /admin/snapshot)
}


def _build_message() -> bytes:
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = STATE["next_header_ts"]
    for e in STATE["entities"]:
        ent = msg.entity.add()
        ent.id = e.get("id", e["trip_id"])
        tu = ent.trip_update
        tu.trip.trip_id = e["trip_id"]
        if e.get("route_id"):
            tu.trip.route_id = e["route_id"]
        if e.get("direction_id") is not None:
            tu.trip.direction_id = int(e["direction_id"])
        if e.get("start_time"):
            tu.trip.start_time = e["start_time"]
        if e.get("cancelled"):
            tu.trip.schedule_relationship = pb.TripDescriptor.CANCELED
            continue
        tu.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
        for st in e.get("stops", []):
            stu = tu.stop_time_update.add()
            stu.stop_sequence = int(st.get("stop_sequence", st.get("seq")))
            if st.get("stop_id"):
                stu.stop_id = st["stop_id"]
            if st.get("arrival", st.get("arr")) is not None:
                stu.arrival.time = int(st.get("arrival", st.get("arr")))
                if st.get("arrival_uncertainty") is not None:
                    stu.arrival.uncertainty = int(st["arrival_uncertainty"])
            if st.get("departure", st.get("dep")) is not None:
                stu.departure.time = int(st.get("departure", st.get("dep")))
            if st.get("skipped"):
                stu.schedule_relationship = (
                    pb.TripUpdate.StopTimeUpdate.SKIPPED
                )
            elif st.get("no_data"):
                stu.schedule_relationship = (
                    pb.TripUpdate.StopTimeUpdate.NO_DATA
                )
            else:
                stu.schedule_relationship = (
                    pb.TripUpdate.StopTimeUpdate.SCHEDULED
                )
    return msg.SerializeToString()


@app.get("/gtfs-rt")
async def get_feed():
    delay = STATE["delay"]
    if delay > 0:
        await asyncio.sleep(delay)
    failure = STATE["failure"]
    if failure == "SLOW":
        await asyncio.sleep(60)
    if failure == "HTTP500":
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=500, content={"error": "simulated"})
    if failure == "EMPTY":
        from fastapi.responses import Response

        return Response(content=b"", media_type="application/x-protobuf")
    if failure == "MALFORMED":
        from fastapi.responses import Response

        return Response(
            content=b"\xff\xff\xff not-a-protobuf",
            media_type="application/x-protobuf",
        )
    if failure == "DROP":
        # Stall beyond any reasonable client timeout, then vanish.
        await asyncio.sleep(120)
    body = _build_message()
    from fastapi.responses import Response

    return Response(content=body, media_type="application/x-protobuf")


@app.post("/admin/snapshot")
async def set_snapshot(payload: dict = Body(...)):
    """Replace the full snapshot.

    Body: {"header_ts": 123, "entities": [{"trip_id":..., "stops":[...]}]}
    Missing header_ts bumps the previous one by 1 so each call looks newer.
    """
    if "header_ts" in payload:
        STATE["next_header_ts"] = int(payload["header_ts"])
    else:
        STATE["next_header_ts"] += 1
    STATE["entities"] = list(payload.get("entities", []))
    return {
        "ok": True,
        "header_ts": STATE["next_header_ts"],
        "entity_count": len(STATE["entities"]),
    }


@app.post("/admin/delay")
async def set_delay(payload: dict = Body(...)):
    STATE["delay"] = float(payload.get("delay", 0.0))
    return {"ok": True, "delay": STATE["delay"]}


@app.post("/admin/failure")
async def set_failure(payload: dict = Body(...)):
    mode = str(payload.get("mode", "NONE")).upper()
    assert mode in {"NONE", "SLOW", "DROP", "MALFORMED", "EMPTY", "HTTP500"}
    STATE["failure"] = mode
    return {"ok": True, "failure": STATE["failure"]}


@app.post("/admin/reset")
async def reset():
    STATE["delay"] = 0.0
    STATE["failure"] = "NONE"
    STATE["entities"] = []
    STATE["next_header_ts"] = int(time.time())
    return {"ok": True}


@app.get("/admin/state")
async def state():
    return STATE
