"""Independent persistent bus-stop sign service.

Stores the *full desired state* pushed by the web app in its own SQLite
database. It is deliberately a separate process/database so the web app
cannot assume a push succeeded just because its local transaction did.

Safety rules
------------
* A push is fully committed **before** any response is sent, so a lost
  receipt still leaves durable state behind.
* Monotonic version guard: an older version is rejected with HTTP 409
  and can never roll the sign back.
* Same version + same idempotency key/hash is an idempotent no-op.
* Receipt loss is simulated *after* commit (hang or HTTP 500), enabled
  at runtime via /admin/lose-receipt.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path

from fastapi import Body, FastAPI, Header, Request
from fastapi.responses import HTMLResponse, JSONResponse

DB_PATH = os.environ.get("SIGN_DB_PATH", "/data/sign.db")

app = FastAPI(title="独立持久化站牌服务")
_LOCK = threading.RLock()
_CONN: sqlite3.Connection | None = None

# Receipt-loss simulation.
# mode: OFF | HANG | HTTP500 ; idem_keys: only lose for these keys (empty = any)
_RL = {"mode": "OFF", "idem_keys": set()}


def _db() -> sqlite3.Connection:
    global _CONN
    if _CONN is None:
        Path(os.path.dirname(DB_PATH) or ".").mkdir(parents=True, exist_ok=True)
        _CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
        _CONN.row_factory = sqlite3.Row
        _CONN.execute("PRAGMA journal_mode=WAL")
        _CONN.executescript(
            """
            CREATE TABLE IF NOT EXISTS sign_state (
                id INTEGER PRIMARY KEY CHECK (id=1),
                version INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                stored_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sign_history (
                version INTEGER PRIMARY KEY,
                idempotency_key TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                stored_at REAL NOT NULL
            );
            """
        )
        _CONN.commit()
    return _CONN


def _current() -> dict | None:
    row = _db().execute("SELECT * FROM sign_state WHERE id=1").fetchone()
    if row is None:
        return None
    return dict(row)


@app.get("/health")
async def health():
    return {"ok": True, "has_state": _current() is not None}


@app.get("/state")
async def get_state():
    cur = _current()
    if cur is None:
        return JSONResponse(
            status_code=404,
            content={"version": None, "message": "站牌尚无生效内容"},
        )
    payload = json.loads(cur["payload_json"])
    payload["_stored_at"] = cur["stored_at"]
    payload["_idempotency_key"] = cur["idempotency_key"]
    return payload


@app.post("/state")
async def put_state(
    request: Request,
    idempotency_key: str | None = Header(default=None),
    content_hash: str | None = Header(default=None),
):
    payload = await request.json()
    version = payload.get("version")
    if not isinstance(version, int):
        return JSONResponse(status_code=422, content={"error": "version 缺失"})
    payload_hash = payload.get("content_hash") or content_hash or ""
    key = idempotency_key or payload.get("idempotency_key") or f"v{version}"

    with _LOCK:
        cur = _current()
        if cur is not None:
            if version < cur["version"]:
                # Late old push: never roll the sign back.
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": "stale version",
                        "current_version": cur["version"],
                        "rejected_version": version,
                    },
                )
            if version == cur["version"]:
                if cur["idempotency_key"] == key or cur["content_hash"] == payload_hash:
                    return JSONResponse(  # idempotent replay
                        status_code=200,
                        content={
                            "version": version,
                            "content_hash": cur["content_hash"],
                            "idempotent": True,
                        },
                    )
                return JSONResponse(
                    status_code=409,
                    content={"error": "same version, different content"},
                )

        now = time.time()
        db = _db()
        db.execute(
            "INSERT INTO sign_state(id, version, idempotency_key, content_hash,"
            " payload_json, stored_at) VALUES(1,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET version=excluded.version, "
            "idempotency_key=excluded.idempotency_key, "
            "content_hash=excluded.content_hash, "
            "payload_json=excluded.payload_json, stored_at=excluded.stored_at",
            (version, key, payload_hash, json.dumps(payload, ensure_ascii=False), now),
        )
        db.execute(
            "INSERT OR REPLACE INTO sign_history(version, idempotency_key, "
            "content_hash, stored_at) VALUES(?,?,?,?)",
            (version, key, payload_hash, now),
        )
        db.commit()  # durable BEFORE the (possibly lost) receipt

    # ---- receipt loss simulation happens strictly after commit ----
    match = not _RL["idem_keys"] or key in _RL["idem_keys"]
    if _RL["mode"] != "OFF" and match:
        _RL["idem_keys"].discard(key)
        if _RL["mode"] == "HANG":
            # The state above is already durable. In a real deployment the
            # response would stall until the client timeout; test transports
            # emulate that lost response, so here we simply finish the handler.
            pass
        return JSONResponse(
            status_code=500,
            content={"error": "simulated receipt loss AFTER commit"},
        )
    return JSONResponse(
        status_code=201,
        content={"version": version, "content_hash": payload_hash},
    )


@app.post("/admin/lose-receipt")
async def lose_receipt(payload: dict = Body(...)):
    mode = str(payload.get("mode", "OFF")).upper()
    assert mode in {"OFF", "HANG", "HTTP500"}
    _RL["mode"] = mode
    keys = payload.get("idem_keys") or []
    _RL["idem_keys"] = set(keys)
    return {"ok": True, "mode": _RL["mode"], "idem_keys": sorted(_RL["idem_keys"])}


@app.post("/admin/reset")
async def reset():
    with _LOCK:
        db = _db()
        db.execute("DELETE FROM sign_state")
        db.execute("DELETE FROM sign_history")
        db.commit()
    _RL["mode"] = "OFF"
    _RL["idem_keys"].clear()
    return {"ok": True}


@app.get("/admin/history")
async def history():
    rows = [
        dict(r)
        for r in _db().execute(
            "SELECT * FROM sign_history ORDER BY version DESC LIMIT 20"
        ).fetchall()
    ]
    return {"history": rows, "receipt_loss": {"mode": _RL["mode"],
                                              "idem_keys": sorted(_RL["idem_keys"])}}


@app.get("/", response_class=HTMLResponse)
async def page():
    return PAGE_HTML


PAGE_HTML = (Path(__file__).parent / "static" / "index.html").read_text(
    encoding="utf-8"
)
