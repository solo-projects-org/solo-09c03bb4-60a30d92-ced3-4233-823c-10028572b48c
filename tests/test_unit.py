"""Unit tests for parsers and storage semantics."""
import pytest

from webapp.gtfs_rt import SnapshotParseError, parse_snapshot
from webapp.payload import build_payload, content_hash_of, ST_PENDING, ST_PREDICTED, ST_CANCELLED, ST_SKIPPED
from webapp.schedule_io import ScheduleError, parse_schedule_csv
from google.transit import gtfs_realtime_pb2 as pb


# ------------------------------------------------------------- schedule CSV
def test_csv_basic_and_date_inference():
    text = "trip_id,stop_sequence,stop_code,scheduled_time\nT1,1,A,08:00\nT1,2,B,08:05\n"
    date, rows = parse_schedule_csv(text, "2026-11-16")
    assert date == "2026-11-16"
    assert rows[0]["scheduled_time"] == "2026-11-16T08:00:00Z"
    assert rows[1]["stop_code"] == "B"


def test_csv_chinese_headers_iso_and_epoch():
    text = "班次,站序,站号,计划时刻\nX,1,A1,2026-11-16T10:00:00Z\nX,2,A2,1794798000\n"
    date, rows = parse_schedule_csv(text, None)
    assert date == "2026-11-16"
    assert rows[0]["scheduled_time"] == "2026-11-16T10:00:00Z"
    import datetime as dt
    assert rows[1]["scheduled_time"] == dt.datetime.fromtimestamp(
        1794798000, dt.timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_csv_rejects_duplicate_and_bad_time():
    text = "trip_id,stop_sequence,stop_code,scheduled_time\nT,1,A,08:00\nT,1,B,08:01\n"
    with pytest.raises(ScheduleError):
        parse_schedule_csv(text, "2026-11-16")
    with pytest.raises(ScheduleError):
        parse_schedule_csv("trip_id,stop_sequence,stop_code,scheduled_time\n", "2026-11-16")
    with pytest.raises(ScheduleError):
        parse_schedule_csv(
            "trip_id,stop_sequence,stop_code,scheduled_time\nT,1,A,25:00\n",
            "2026-11-16",
        )


def test_csv_rejects_wrong_service_day():
    text = "trip_id,stop_sequence,stop_code,scheduled_time\nT,1,A,2026-11-15T08:00Z\n"
    with pytest.raises(ScheduleError):
        parse_schedule_csv(text, "2026-11-16")


# ------------------------------------------------------------- GTFS-RT parse
def _feed(header_ts, entities):
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = header_ts
    for e in entities:
        ent = msg.entity.add()
        ent.id = e["trip_id"]
        tu = ent.trip_update
        tu.trip.trip_id = e["trip_id"]
        if e.get("cancelled"):
            tu.trip.schedule_relationship = pb.TripDescriptor.CANCELED
            continue
        tu.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
        for st in e.get("stops", []):
            stu = tu.stop_time_update.add()
            stu.stop_sequence = st["seq"]
            if "arr" in st:
                stu.arrival.time = st["arr"]
            if "dep" in st:
                stu.departure.time = st["dep"]
            if st.get("skipped"):
                stu.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SKIPPED
            if st.get("no_data"):
                stu.schedule_relationship = pb.TripUpdate.StopTimeUpdate.NO_DATA
    return msg.SerializeToString()


def test_parse_filters_unknown_trips_and_absolute_times():
    body = _feed(1000, [
        {"trip_id": "T1", "stops": [{"seq": 1, "arr": 1100, "dep": 1101},
                                    {"seq": 2, "no_data": True}]},
        {"trip_id": "UNKNOWN", "stops": []},
    ])
    parsed = parse_snapshot(body, {"T1"})
    assert parsed.header_ts == 1000
    assert parsed.ignored_count == 1
    assert len(parsed.entities) == 1
    ent = parsed.entities[0]
    assert len(ent.stops) == 1  # NO_DATA stop dropped -> PENDING
    assert ent.stops[0].arrival == 1100 and ent.stops[0].departure == 1101


def test_parse_cancellation():
    body = _feed(1000, [{"trip_id": "T1", "cancelled": True}])
    parsed = parse_snapshot(body, {"T1"})
    assert parsed.entities[0].cancelled is True


def test_parse_filters_foreign_service_date():
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1000
    ent = msg.entity.add()
    ent.id = "T1"
    ent.trip_update.trip.trip_id = "T1"
    ent.trip_update.trip.start_date = "20261115"  # not the 16th
    ent.trip_update.stop_time_update.add().stop_sequence = 1
    parsed = parse_snapshot(msg.SerializeToString(), {"T1"}, "2026-11-16")
    assert parsed.ignored_count == 1 and parsed.entities == []

    # Same-day entity passes the filter.
    ent.trip_update.trip.start_date = "20261116"
    parsed = parse_snapshot(msg.SerializeToString(), {"T1"}, "2026-11-16")
    assert len(parsed.entities) == 1


def test_parse_errors():
    with pytest.raises(SnapshotParseError):
        parse_snapshot(b"", {"T1"})
    with pytest.raises(SnapshotParseError):
        parse_snapshot(b"\xff\xff garbage", {"T1"})
    msg = pb.FeedMessage()
    # header present but without timestamp -> invalid
    msg.header.gtfs_realtime_version = "2.0"
    with pytest.raises(SnapshotParseError):
        parse_snapshot(msg.SerializeToString(), set())


# ------------------------------------------------------------- storage rules
def test_snapshot_revokes_missing_trip_to_pending(storage):
    storage.replace_schedule("2026-11-16", [
        {"trip_id": "T1", "stop_sequence": 1, "stop_code": "A",
         "scheduled_time": "2026-11-16T08:00:00Z"},
        {"trip_id": "T2", "stop_sequence": 1, "stop_code": "A",
         "scheduled_time": "2026-11-16T09:00:00Z"},
    ])
    storage.apply_snapshot(1000, "h1", [
        {"trip_id": "T1", "cancelled": False, "stops": [
            {"stop_sequence": 1, "arrival": 1100, "departure": 1101,
             "uncertainty": None, "sched_rel": "SCHEDULED"}]},
        {"trip_id": "T2", "cancelled": True, "stops": []},
    ], 0)
    all_rows = storage.get_trip_rows()
    t2_rows = [r for r in all_rows if r["trip_id"] == "T2"]
    t1_rows = [r for r in all_rows if r["trip_id"] == "T1"]
    assert all(r["cancelled"] == 1 for r in t2_rows)
    assert t1_rows[0]["arrival_time"] == 1100

    # New complete snapshot without T2 -> T2 realtime revoked -> PENDING
    storage.apply_snapshot(2000, "h2", [
        {"trip_id": "T1", "cancelled": False, "stops": [
            {"stop_sequence": 1, "arrival": 1200, "departure": 1201,
             "uncertainty": None, "sched_rel": "SCHEDULED"}]},
    ], 0)
    all_rows = storage.get_trip_rows()
    t2_rows = [r for r in all_rows if r["trip_id"] == "T2"]
    assert all(not r["cancelled"] and r["arrival_time"] is None for r in t2_rows)
    t1_rows = [r for r in all_rows if r["trip_id"] == "T1"]
    assert t1_rows[0]["arrival_time"] == 1200

    payload = build_payload(storage, storage.get_desired_version())
    t2 = next(t for t in payload["trips"] if t["trip_id"] == "T2")
    assert t2["cancelled"] is False
    assert t2["stops"][0]["status"] == ST_PENDING
    t1 = next(t for t in payload["trips"] if t["trip_id"] == "T1")
    assert t1["stops"][0]["status"] == ST_PREDICTED
    # payload hash is canonical
    assert content_hash_of(payload) == payload["content_hash"]


def test_superseded_attempts_and_cancel_payload(storage):
    storage.replace_schedule("2026-11-16", [
        {"trip_id": "T1", "stop_sequence": 1, "stop_code": "A",
         "scheduled_time": "2026-11-16T08:00:00Z"}])
    # The plan upload itself is desired-state v1; each applied snapshot
    # bumps the same monotonic counter the sign sees.
    assert storage.get_desired_version() == 1
    storage.apply_snapshot(1, "h1", [], 0)
    storage.apply_snapshot(2, "h2", [
        {"trip_id": "T1", "cancelled": True, "stops": []}], 0)
    attempts = storage.get_push_attempts()
    statuses = {a["version"]: a["status"] for a in attempts}
    assert statuses == {1: "SUPERSEDED", 2: "SUPERSEDED", 3: "PENDING"}
    payload = build_payload(storage, 3)
    assert payload["trips"][0]["cancelled"] is True
    assert payload["trips"][0]["stops"][0]["status"] == ST_CANCELLED


def test_skipped_stop_status(storage):
    storage.replace_schedule("2026-11-16", [
        {"trip_id": "T1", "stop_sequence": 1, "stop_code": "A",
         "scheduled_time": "2026-11-16T08:00:00Z"},
        {"trip_id": "T1", "stop_sequence": 2, "stop_code": "B",
         "scheduled_time": "2026-11-16T08:05:00Z"}])
    storage.apply_snapshot(1, "h1", [
        {"trip_id": "T1", "cancelled": False, "stops": [
            {"stop_sequence": 2, "arrival": None, "departure": None,
             "uncertainty": None, "sched_rel": "SKIPPED"}]}], 0)
    payload = build_payload(storage, storage.get_desired_version())
    stops = {s["stop_sequence"]: s for s in payload["trips"][0]["stops"]}
    assert stops[1]["status"] == ST_PENDING
    assert stops[2]["status"] == ST_SKIPPED


def test_replace_schedule_resets_realtime_and_enqueues_plan_push(storage):
    storage.replace_schedule("2026-11-16", [
        {"trip_id": "T1", "stop_sequence": 1, "stop_code": "A",
         "scheduled_time": "2026-11-16T08:00:00Z"}])
    storage.apply_snapshot(1, "h1", [], 0)
    storage.replace_schedule("2026-11-17", [
        {"trip_id": "X", "stop_sequence": 1, "stop_code": "Z",
         "scheduled_time": "2026-11-17T08:00:00Z"}])
    assert storage.get_service_date() == "2026-11-17"
    assert storage.latest_applied_snapshot() is None
    # Realtime state is reset, but the corrected plan itself is enqueued
    # as the newest desired state so it reaches the sign without waiting
    # for a new realtime snapshot.
    attempts = storage.get_push_attempts()
    assert len(attempts) == 1
    assert attempts[0]["status"] == "PENDING"
    assert attempts[0]["idem_key"].startswith("plan-")
    # The desired-state version is monotonic across plan replacements.
    assert attempts[0]["version"] == storage.get_desired_version() == 3
    summary = storage.get_sync_summary()
    assert summary["desired_kind"] == "plan"
    assert summary["rt_version"] is None and summary["latest_header_ts"] is None
    # No collection success is faked for the plan-only state.
    assert summary["poll_state"] == "NO_DATA"
    assert summary["last_success_at"] is None


def test_migration_from_pre_upgrade_volume(tmp_path):
    """Old data volumes keep working: the desired-version counter is
    seeded above anything the sign may already have received, and the
    new payload_json column is added in place."""
    import sqlite3 as _sql

    db = str(tmp_path / "old.db")
    conn = _sql.connect(db)
    # Schema as it existed before the upgrade (no payload_json column).
    conn.executescript("""
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE schedule (trip_id TEXT NOT NULL, stop_sequence INTEGER NOT NULL,
        stop_code TEXT NOT NULL, scheduled_time TEXT NOT NULL,
        PRIMARY KEY (trip_id, stop_sequence));
    CREATE TABLE rt_state (trip_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
        header_ts INTEGER NOT NULL, cancelled INTEGER NOT NULL DEFAULT 0,
        route_id TEXT, direction_id INTEGER, trip_start_time TEXT,
        updated_at REAL NOT NULL);
    CREATE TABLE rt_stop (trip_id TEXT NOT NULL, stop_sequence INTEGER NOT NULL,
        arrival_time INTEGER, departure_time INTEGER, uncertainty INTEGER,
        sched_rel TEXT, PRIMARY KEY (trip_id, stop_sequence));
    CREATE TABLE snapshot (version INTEGER PRIMARY KEY AUTOINCREMENT,
        header_ts INTEGER NOT NULL, body_hash TEXT NOT NULL,
        applied INTEGER NOT NULL, reject_reason TEXT,
        entity_count INTEGER NOT NULL, ignored_count INTEGER NOT NULL DEFAULT 0,
        received_at REAL NOT NULL);
    CREATE TABLE push_attempt (id INTEGER PRIMARY KEY AUTOINCREMENT,
        version INTEGER NOT NULL, idem_key TEXT NOT NULL UNIQUE,
        content_hash TEXT NOT NULL, status TEXT NOT NULL, http_status INTEGER,
        error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
    """)
    for i, ts in enumerate((1000, 2000, 3000), start=1):
        conn.execute(
            "INSERT INTO snapshot(header_ts, body_hash, applied, entity_count,"
            " received_at) VALUES (?,?,1,0,?)", (ts, f"h{i}", float(i)))
    conn.execute(
        "INSERT INTO push_attempt(version, idem_key, content_hash, status,"
        " created_at, updated_at) VALUES (3,'snap-3-abc','h3','CONFIRMED',3.0,3.0)")
    conn.execute("INSERT INTO meta(key,value) VALUES('service_date','2026-11-16')")
    conn.commit()
    conn.close()

    from webapp.storage import Storage

    s = Storage(db)  # migration runs here
    try:
        # Seeded above anything the sign may already serve (v3).
        assert s.get_desired_version() == 3
        s.replace_schedule("2026-11-16", [
            {"trip_id": "T1", "stop_sequence": 1, "stop_code": "A",
             "scheduled_time": "2026-11-16T08:00:00Z"}])
        attempt = s.get_push_attempts()[0]
        # Strictly newer than any pre-upgrade push: never rejected by the
        # sign's monotonic guard.
        assert attempt["version"] == 4
        claimed = s.claim_next_push()
        assert claimed["payload_json"] is not None
        import json as _json

        assert _json.loads(claimed["payload_json"])["version"] == 4
    finally:
        s.close()
