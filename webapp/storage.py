"""SQLite persistence layer.

One connection guarded by a lock is enough for the single-process
FastAPI app; synchronous methods are called from the threadpool / via
``asyncio.to_thread`` from background workers.

Design notes
------------
* A *snapshot version* is a monotonically increasing integer assigned every
  time a full GTFS-RT snapshot is observed. Applied snapshots replace the
  whole realtime state in one transaction.
* Trips absent from an applied snapshot simply have no ``rt_state`` row, so
  the schedule/rt LEFT JOIN renders them as PENDING — this is how
  "revoked realtime status -> 待定" is expressed without extra flags.
* Push attempts carry a stable idempotency key and the content hash of the
  full detail payload, allowing reconciliation after lost receipts,
  restarts and concurrent pushes.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule (
    trip_id       TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_code     TEXT NOT NULL,
    scheduled_time TEXT NOT NULL,
    PRIMARY KEY (trip_id, stop_sequence)
);
CREATE TABLE IF NOT EXISTS rt_state (
    trip_id        TEXT PRIMARY KEY,
    version        INTEGER NOT NULL,
    header_ts      INTEGER NOT NULL,
    cancelled      INTEGER NOT NULL DEFAULT 0,
    route_id       TEXT,
    direction_id   INTEGER,
    trip_start_time TEXT,
    updated_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS rt_stop (
    trip_id        TEXT NOT NULL,
    stop_sequence  INTEGER NOT NULL,
    arrival_time   INTEGER,
    departure_time INTEGER,
    uncertainty    INTEGER,
    sched_rel      TEXT,
    PRIMARY KEY (trip_id, stop_sequence)
);
CREATE TABLE IF NOT EXISTS snapshot (
    version      INTEGER PRIMARY KEY AUTOINCREMENT,
    header_ts    INTEGER NOT NULL,
    body_hash    TEXT NOT NULL,
    applied      INTEGER NOT NULL,
    reject_reason TEXT,
    entity_count INTEGER NOT NULL,
    ignored_count INTEGER NOT NULL DEFAULT 0,
    received_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS push_attempt (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER NOT NULL,
    idem_key    TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    status      TEXT NOT NULL,
    http_status INTEGER,
    error       TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_push_version ON push_attempt(version);
CREATE INDEX IF NOT EXISTS idx_schedule_trip ON schedule(trip_id);
"""

# push_attempt.status values
P_PENDING = "PENDING"
P_IN_FLIGHT = "IN_FLIGHT"
P_CONFIRMED = "CONFIRMED"
P_FAILED = "FAILED"
P_SUPERSEDED = "SUPERSEDED"


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        cur = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = cur.fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: Any) -> None:
        self._conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        self._conn.commit()

    def get_service_date(self) -> str | None:
        return self.get_meta("service_date")

    # -------------------------------------------------------------- schedule
    def replace_schedule(self, service_date: str, rows: Iterable[dict]) -> int:
        """Atomically load a new service day and reset all derived state."""
        rows = list(rows)
        with self._lock:
            cur = self._conn
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute("DELETE FROM rt_stop")
                cur.execute("DELETE FROM rt_state")
                cur.execute("DELETE FROM push_attempt")
                cur.execute("DELETE FROM snapshot")
                cur.execute("DELETE FROM schedule")
                cur.execute("DELETE FROM meta")
                cur.executemany(
                    "INSERT INTO schedule(trip_id, stop_sequence, stop_code, "
                    "scheduled_time) VALUES(?,?,?,?)",
                    [
                        (
                            r["trip_id"],
                            int(r["stop_sequence"]),
                            str(r["stop_code"]),
                            r["scheduled_time"],
                        )
                        for r in rows
                    ],
                )
                cur.execute(
                    "INSERT INTO meta(key,value) VALUES('service_date',?)",
                    (service_date,),
                )
                cur.execute(
                    "INSERT INTO meta(key,value) VALUES('poll_state',?)",
                    ("NO_DATA",),
                )
                cur.execute(
                    "INSERT INTO meta(key,value) VALUES('push_state',?)",
                    ("IDLE",),
                )
                cur.commit()
            except Exception:
                cur.rollback()
                raise
        return len(rows)

    def list_schedule_trip_ids(self) -> set[str]:
        cur = self._conn.execute("SELECT DISTINCT trip_id FROM schedule")
        return {r["trip_id"] for r in cur.fetchall()}

    # -------------------------------------------------------------- snapshot
    def latest_snapshot(self) -> sqlite3.Row | None:
        cur = self._conn.execute(
            "SELECT * FROM snapshot ORDER BY version DESC LIMIT 1"
        )
        return cur.fetchone()

    def latest_applied_snapshot(self) -> sqlite3.Row | None:
        cur = self._conn.execute(
            "SELECT * FROM snapshot WHERE applied=1 ORDER BY version DESC LIMIT 1"
        )
        return cur.fetchone()

    def record_rejected_snapshot(
        self, header_ts: int, body_hash: str, reason: str, entity_count: int
    ) -> int:
        with self._lock:
            now = time.time()
            cur = self._conn.execute(
                "INSERT INTO snapshot(header_ts, body_hash, applied, "
                "reject_reason, entity_count, received_at) "
                "VALUES(?,?,0,?,?,?)",
                (header_ts, body_hash, reason, entity_count, now),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def apply_snapshot(
        self,
        header_ts: int,
        body_hash: str,
        entities: list[dict],
        ignored_count: int,
    ) -> int:
        """Replace all realtime state with a complete snapshot.

        ``entities`` only contains trips present in the loaded schedule.
        Trips of the day that are missing from the snapshot get no rt rows,
        which the UI interprets as PENDING (old realtime status revoked).
        Creates a PENDING push attempt for the new version and supersedes
        every attempt for older versions.
        """
        now = time.time()
        with self._lock:
            db = self._conn
            db.execute("BEGIN IMMEDIATE")
            try:
                cur = db.execute(
                    "INSERT INTO snapshot(header_ts, body_hash, applied, "
                    "entity_count, ignored_count, received_at) "
                    "VALUES(?,?,1,?,?,?)",
                    (header_ts, body_hash, len(entities), ignored_count, now),
                )
                version = int(cur.lastrowid)
                db.execute("DELETE FROM rt_stop")
                db.execute("DELETE FROM rt_state")
                for ent in entities:
                    db.execute(
                        "INSERT INTO rt_state(trip_id, version, header_ts, "
                        "cancelled, route_id, direction_id, trip_start_time, "
                        "updated_at) VALUES(?,?,?,?,?,?,?,?)",
                        (
                            ent["trip_id"],
                            version,
                            header_ts,
                            1 if ent["cancelled"] else 0,
                            ent.get("route_id"),
                            ent.get("direction_id"),
                            ent.get("trip_start_time"),
                            now,
                        ),
                    )
                    if not ent["cancelled"]:
                        db.executemany(
                            "INSERT INTO rt_stop(trip_id, stop_sequence, "
                            "arrival_time, departure_time, uncertainty, "
                            "sched_rel) VALUES(?,?,?,?,?,?)",
                            [
                                (
                                    ent["trip_id"],
                                    int(s["stop_sequence"]),
                                    s.get("arrival"),
                                    s.get("departure"),
                                    s.get("uncertainty"),
                                    s.get("sched_rel"),
                                )
                                for s in ent["stops"]
                            ],
                        )
                # Old pushes can never become the desired state anymore.
                db.execute(
                    "UPDATE push_attempt SET status=?, updated_at=? "
                    "WHERE status IN (?,?,?)",
                    (P_SUPERSEDED, now, P_PENDING, P_IN_FLIGHT, P_FAILED),
                )
                idem_key = f"snap-{version}-{body_hash[:12]}"
                db.execute(
                    "INSERT INTO push_attempt(version, idem_key, content_hash,"
                    " status, created_at, updated_at) VALUES(?,?,?,?,?,?)",
                    (version, idem_key, body_hash, P_PENDING, now, now),
                )
                db.execute(
                    "INSERT INTO meta(key,value) VALUES('latest_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (version,),
                )
                db.execute(
                    "INSERT INTO meta(key,value) VALUES('poll_state',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("OK",),
                )
                db.execute(
                    "INSERT INTO meta(key,value) VALUES('last_success_at',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (now,),
                )
                db.execute(
                    "INSERT INTO meta(key,value) VALUES('last_error',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("",),
                )
                db.commit()
                return version
            except Exception:
                db.rollback()
                raise

    # ------------------------------------------------------------- poll state
    def record_poll_error(self, message: str) -> None:
        with self._lock:
            now = time.time()
            self.set_meta("poll_state", "ERROR")
            self.set_meta("last_error", message[:500])
            self.set_meta("last_error_at", now)
            self.set_meta("last_poll_at", now)

    def record_poll_attempt_ok(self) -> None:
        with self._lock:
            self.set_meta("last_poll_at", time.time())

    # ------------------------------------------------------------- push state
    def reset_inflight_on_startup(self) -> None:
        """An IN_FLIGHT attempt may have completed without a receipt.

        Mark it FAILED with an explicit note: the outcome is unknown after
        a crash. Reconciliation can then confirm it by content hash, or the
        push loop requeues it for an idempotent retry.
        """
        with self._lock:
            now = time.time()
            self._conn.execute(
                "UPDATE push_attempt SET status=?, error=?, updated_at=? "
                "WHERE status=?",
                (P_FAILED, "应用重启：中断的在途推送，待核实/重试", now, P_IN_FLIGHT),
            )
            self._conn.commit()

    def claim_next_push(self) -> sqlite3.Row | None:
        with self._lock:
            db = self._conn
            row = db.execute(
                "SELECT * FROM push_attempt WHERE status=? "
                "ORDER BY version DESC LIMIT 1",
                (P_PENDING,),
            ).fetchone()
            if row is None:
                return None
            db.execute(
                "UPDATE push_attempt SET status=?, updated_at=? WHERE id=?",
                (P_IN_FLIGHT, time.time(), row["id"]),
            )
            db.commit()
            return db.execute(
                "SELECT * FROM push_attempt WHERE id=?", (row["id"],)
            ).fetchone()

    def mark_push_confirmed(self, attempt_id: int, http_status: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE push_attempt SET status=?, http_status=?, "
                "error=NULL, updated_at=? WHERE id=?",
                (P_CONFIRMED, http_status, time.time(), attempt_id),
            )
            self._conn.commit()

    def mark_push_failed(
        self, attempt_id: int, http_status: int | None, error: str
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE push_attempt SET status=?, http_status=?, error=?, "
                "updated_at=? WHERE id=?",
                (P_FAILED, http_status, error[:500], time.time(), attempt_id),
            )
            self._conn.commit()

    def requeue_failed(self) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE push_attempt SET status=?, updated_at=? WHERE status=?",
                (P_PENDING, time.time(), P_FAILED),
            )
            self._conn.commit()

    def mark_push_superseded(self, attempt_id: int, detail: str) -> None:
        """The sign already serves a newer version; stop retrying this one."""
        with self._lock:
            self._conn.execute(
                "UPDATE push_attempt SET status=?, error=?, updated_at=? WHERE id=?",
                (P_SUPERSEDED, detail[:500], time.time(), attempt_id),
            )
            self._conn.commit()

    def get_attempt(self, version: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM push_attempt WHERE version=? ORDER BY id DESC LIMIT 1",
            (version,),
        ).fetchone()

    def update_attempt_content_hash(self, attempt_id: int, content_hash: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE push_attempt SET content_hash=?, updated_at=? WHERE id=?",
                (content_hash, time.time(), attempt_id),
            )
            self._conn.commit()

    def record_sign_observation(
        self,
        reachable: bool,
        version: int | None,
        content_hash: str | None,
        detail: str = "",
    ) -> None:
        with self._lock:
            now = time.time()
            self.set_meta("sign_reachable", "1" if reachable else "0")
            self.set_meta("sign_observed_at", now)
            if version is not None:
                self.set_meta("sign_observed_version", version)
            if content_hash is not None:
                self.set_meta("sign_observed_hash", content_hash)
            if detail:
                self.set_meta("sign_observation_detail", detail[:500])
            # Reconcile: if the sign already serves this exact version,
            # confirm an attempt that was actually dispatched (IN_FLIGHT /
            # FAILED, e.g. receipt lost) even without its receipt. A still
            # PENDING attempt has never been sent, so it stays queued.
            if version is not None and content_hash is not None:
                attempt = self.get_attempt(version)
                if (
                    attempt is not None
                    and attempt["content_hash"] == content_hash
                    and attempt["status"] in (P_IN_FLIGHT, P_FAILED)
                ):
                    self._conn.execute(
                        "UPDATE push_attempt SET status=?, "
                        "http_status=COALESCE(http_status, 200), "
                        "error=NULL, updated_at=? WHERE id=?",
                        (P_CONFIRMED, now, attempt["id"]),
                    )
                    self._conn.commit()

    # --------------------------------------------------------------- queries
    def get_trip_rows(self) -> list[dict]:
        """Full per-stop detail joining schedule and latest realtime state."""
        sql = """
        SELECT s.trip_id, s.stop_sequence, s.stop_code, s.scheduled_time,
               rs.cancelled, rs.version AS rt_version, rs.header_ts,
               rs.route_id, rs.direction_id,
               rp.arrival_time, rp.departure_time, rp.uncertainty, rp.sched_rel
        FROM schedule s
        LEFT JOIN rt_state rs ON rs.trip_id = s.trip_id
        LEFT JOIN rt_stop  rp ON rp.trip_id = s.trip_id
                                AND rp.stop_sequence = s.stop_sequence
        ORDER BY s.trip_id, s.stop_sequence
        """
        return [dict(r) for r in self._conn.execute(sql).fetchall()]

    def get_snapshots(self) -> list[dict]:
        return [
            dict(r)
            for r in self._conn.execute(
                "SELECT * FROM snapshot ORDER BY version DESC LIMIT 50"
            ).fetchall()
        ]

    def get_push_attempts(self) -> list[dict]:
        return [
            dict(r)
            for r in self._conn.execute(
                "SELECT * FROM push_attempt ORDER BY id DESC LIMIT 50"
            ).fetchall()
        ]

    def get_sync_summary(self) -> dict:
        m = {r["key"]: r["value"] for r in self._conn.execute("SELECT * FROM meta")}
        latest = self.latest_applied_snapshot()
        pending = self._conn.execute(
            "SELECT COUNT(*) c FROM push_attempt WHERE status IN (?,?)",
            (P_PENDING, P_IN_FLIGHT),
        ).fetchone()["c"]
        return {
            "poll_state": m.get("poll_state", "NO_DATA"),
            "last_poll_at": _maybe_float(m.get("last_poll_at")),
            "last_success_at": _maybe_float(m.get("last_success_at")),
            "last_error_at": _maybe_float(m.get("last_error_at")),
            "last_error": m.get("last_error", ""),
            "latest_version": int(latest["version"]) if latest else None,
            "latest_header_ts": latest["header_ts"] if latest else None,
            "pending_push_count": pending,
            "sign_reachable": m.get("sign_reachable") == "1",
            "sign_observed_version": _maybe_int(m.get("sign_observed_version")),
            "sign_observed_hash": m.get("sign_observed_hash"),
            "sign_observed_at": _maybe_float(m.get("sign_observed_at")),
            "sign_observation_detail": m.get("sign_observation_detail", ""),
        }


def _maybe_float(v: str | None) -> float | None:
    return float(v) if v not in (None, "") else None


def _maybe_int(v: str | None) -> int | None:
    return int(v) if v not in (None, "") else None
