"""End-to-end smoke test against three REAL uvicorn processes (real sockets,
real timeouts, real SQLite files). Run run_local.sh first, then:

    .venv/bin/python scripts/smoke_e2e.py
"""
from __future__ import annotations

import sys
import time

import httpx

WEB = "http://127.0.0.1:8000"
GW = "http://127.0.0.1:8001"
SIGN = "http://127.0.0.1:8002"

CSV = """trip_id,stop_sequence,stop_code,scheduled_time
T1,1,ST-A,08:00:00
T1,2,ST-B,08:05:00
T1,3,ST-C,08:10:00
T2,1,ST-A,09:00:00
T2,2,ST-B,09:05:00
"""

# Corrected plan: same trips, new stop codes (the data-worker fix).
CSV_CORRECTED = """trip_id,stop_sequence,stop_code,scheduled_time
T1,1,ST-X,08:00:00
T1,2,ST-Y,08:05:00
T1,3,ST-Z,08:10:00
T2,1,ST-X,09:00:00
T2,2,ST-Y,09:05:00
"""

failures = []


def check(name: str, cond: bool, detail=""):
    print(f"{'✅' if cond else '❌'} {name} {detail}")
    if not cond:
        failures.append(name)


def wait_until(fn, timeout=12, interval=0.4):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    return last


def pred(trip, base, n=3):
    return {
        "trip_id": trip,
        "stops": [
            {"seq": i, "arrival": base + i * 60, "departure": base + i * 60 + 10}
            for i in range(1, n + 1)
        ],
    }


def main():
    web = httpx.Client(base_url=WEB, timeout=8)
    gw = httpx.Client(base_url=GW, timeout=8)
    sign = httpx.Client(base_url=SIGN, timeout=8)

    # Monotonic, run-unique header timestamps so repeated runs never look
    # like late snapshots against history left in the database.
    base_ts = int(time.time())

    # 0. reset
    gw.post("/admin/reset").raise_for_status()
    sign.post("/admin/reset").raise_for_status()

    # 1. upload
    r = web.post(
        "/api/schedule",
        files={"file": ("sched.csv", CSV, "text/csv")},
        data={"service_date": "2026-11-16"},
    )
    check("上传计划 CSV", r.status_code == 200, r.text)
    check("行数=5", r.json().get("row_count") == 5)
    check("上传即产生待同步版本", r.json().get("desired_version") == 1)

    def local_latest():
        return web.get("/api/sync").json()["local_latest"]

    # 2. snapshot v1 -> poll -> push
    gw.post("/admin/snapshot", json={"header_ts": base_ts+1, "entities": [
        pred("T1", 1_700_000_000), pred("T2", 1_700_001_000, 2)]})
    check("手动轮询 v1", web.post("/api/poll-now").json()["outcome"] == "applied")
    check("手动推送", web.post("/api/push-now").json()["outcome"] == "confirmed")
    st = sign.get("/state").json()
    check("站牌生效=本地最新", st["version"] == local_latest(),
          f"sign={st['version']} local={local_latest()}")

    # 3. missing trip -> PENDING
    gw.post("/admin/snapshot", json={"header_ts": base_ts+2, "entities": [pred("T1", 1_700_002_000)]})
    check("轮询 v2", web.post("/api/poll-now").json()["outcome"] == "applied")
    trips = web.get("/api/trips").json()["trips"]
    t2 = next(t for t in trips if t["trip_id"] == "T2")
    check("缺席班次撤销为待定", all(s["status"] == "PENDING" for s in t2["stops"]))

    # 4. manual push catches up v2 (background workers disabled in smoke mode)
    check("手动推送 v2", web.post("/api/push-now").json()["outcome"] == "confirmed")
    check("站牌生效=本地最新", sign.get("/state").json()["version"] == local_latest())

    # 5. malformed snapshot -> stale, state retained
    gw.post("/admin/failure", json={"mode": "MALFORMED"})
    check("坏快照解析失败", web.post("/api/poll-now").json()["outcome"] == "parse_error")
    sync = web.get("/api/sync").json()
    check("标记 STALE 且保留状态", sync["stale"] is True)
    trips = web.get("/api/trips").json()["trips"]
    check("保留上次成功预测",
          next(t for t in trips if t["trip_id"] == "T1")["stops"][0]["status"] == "PREDICTED")
    gw.post("/admin/failure", json={"mode": "NONE"})

    # 6. late snapshot cannot roll back
    gw.post("/admin/snapshot", json={"header_ts": base_ts+1, "entities": [pred("T1", 999_999)]})
    check("迟到快照被拒绝", web.post("/api/poll-now").json()["outcome"] == "stale_ignored")

    # 7. cancellation
    gw.post("/admin/snapshot", json={"header_ts": base_ts+3, "entities": [
        {"trip_id": "T1", "cancelled": True}, pred("T2", 1_700_003_000, 2)]})
    check("取消快照应用", web.post("/api/poll-now").json()["outcome"] == "applied")
    check("推送取消版本", web.post("/api/push-now").json()["outcome"] == "confirmed")
    st = sign.get("/state").json()
    check("站牌版本=本地最新", st["version"] == local_latest(), str(st.get("version")))
    t1 = next(t for t in st["trips"] if t["trip_id"] == "T1")
    check("站牌显示整班取消", t1["cancelled"] and all(s["status"] == "CANCELLED" for s in t1["stops"]))

    # 8. lost receipt after commit (HTTP500 mode), then reconcile
    gw.post("/admin/snapshot", json={"header_ts": base_ts+4, "entities": [pred("T1", 1_700_004_000)]})
    check("轮询新版本", web.post("/api/poll-now").json()["outcome"] == "applied")
    sign.post("/admin/lose-receipt", json={"mode": "HTTP500"})
    out = web.post("/api/push-now").json()
    check("丢回执推送报错", out["outcome"] in ("http_error", "push_error"), str(out))
    check("站牌实际已落库", sign.get("/state").json()["version"] == local_latest())
    sign.post("/admin/lose-receipt", json={"mode": "OFF"})
    rec = web.post("/api/reconcile-now").json()
    check("对账识别站牌版本", rec.get("version") == local_latest())
    sync = web.get("/api/sync").json()
    check("对账后本地确认", sync["in_sync"] is True, str(sync.get("push_attempts", [])[:2]))

    # 9. late old push rejected by sign (409)
    r = sign.post("/state", json={"version": 1, "content_hash": "old", "trips": []},
                  headers={"Idempotency-Key": "old"})
    check("站牌拒绝旧版本回退(409)", r.status_code == 409)
    check("站牌仍是本地最新", sign.get("/state").json()["version"] == local_latest())

    # 10. evidence endpoint
    ev = web.get("/api/evidence/T1").json()
    check("逐站依据可查", len(ev["stops"]) == 3 and "scheduled_time" in ev["stops"][0])

    # 11. plan correction: synced without waiting for a new snapshot
    r = web.post(
        "/api/schedule",
        files={"file": ("sched2.csv", CSV_CORRECTED, "text/csv")},
        data={"service_date": "2026-11-16"},
    )
    check("更正计划上传成功", r.status_code == 200, r.text)
    corrected_v = r.json()["desired_version"]
    trips = web.get("/api/trips").json()
    t1 = next(t for t in trips["trips"] if t["trip_id"] == "T1")
    check("本地新站号待定", [s["stop_code"] for s in t1["stops"]] == ["ST-X", "ST-Y", "ST-Z"]
          and all(s["status"] == "PENDING" for s in t1["stops"]))
    check("本地标记实时待定", trips["realtime_pending"] is True)
    sync = web.get("/api/sync").json()
    check("网页区分待同步与生效", sync["local_latest"] == corrected_v
          and sync["sign_effective_version"] == corrected_v - 1
          and sync["in_sync"] is False,
          f"local={sync['local_latest']} sign={sync['sign_effective_version']}")
    # No new snapshot: one push syncs the corrected plan's PENDING detail.
    out = web.post("/api/push-now").json()
    check("无需新快照即推送更正", out["outcome"] == "confirmed", str(out))
    st = sign.get("/state").json()
    t1 = next(t for t in st["trips"] if t["trip_id"] == "T1")
    check("站牌新站号全待定", st["version"] == corrected_v
          and [s["stop_code"] for s in t1["stops"]] == ["ST-X", "ST-Y", "ST-Z"]
          and all(s["status"] == "PENDING" for s in t1["stops"])
          and st["header_ts"] is None)
    sync = web.get("/api/sync").json()
    check("未冒充采集成功", sync["poll_state"] == "NO_DATA"
          and sync["last_success_at"] is None and sync["stale"] is False)
    check("更正后账实一致", sync["in_sync"] is True)

    # 12. later valid snapshot is not blocked by the plan push
    gw.post("/admin/snapshot", json={"header_ts": base_ts+5, "entities": [
        pred("T1", 1_700_005_000), pred("T2", 1_700_006_000, 2)]})
    check("后续有效快照仍应用", web.post("/api/poll-now").json()["outcome"] == "applied")
    check("推送快照版本", web.post("/api/push-now").json()["outcome"] == "confirmed")
    st = sign.get("/state").json()
    t1 = next(t for t in st["trips"] if t["trip_id"] == "T1")
    check("站牌新站号+新预测", t1["stops"][0]["stop_code"] == "ST-X"
          and t1["stops"][0]["status"] == "PREDICTED")

    print()
    if failures:
        print(f"FAILED {len(failures)}: {failures}")
        sys.exit(1)
    print("ALL SMOKE CHECKS PASSED")


if __name__ == "__main__":
    main()
