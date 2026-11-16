"""Background synchronization: GTFS-RT polling and sign pushing.

Two independent asyncio loops with different failure semantics:

* **poller** -- every interval fetch a *complete* snapshot. Fetch/parse
  failures never touch the last successfully applied state; they only
  flip a STALE flag. A snapshot whose header timestamp is not newer than
  the last applied one is ignored (late snapshots cannot roll back).
* **pusher** -- keeps pushing the latest desired payload until the sign
  confirms it. Pushing failures never block polling. Before every push
  it checks what the sign actually serves and reconciles lost receipts;
  the sign itself rejects stale versions so a late old push can never
  roll its state back. The payload is frozen when the attempt is claimed,
  so content, hash and receipt always belong to the same desired state.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time

import httpx

from .config import Settings
from .gtfs_rt import SnapshotParseError, parse_snapshot
from .storage import Storage


class Poller:
    def __init__(self, storage: Storage, settings: Settings, transport=None):
        self.storage = storage
        self.settings = settings
        self._transport = transport
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    async def poll_once(self, client: httpx.AsyncClient) -> dict:
        """One fetch+apply cycle. Returns a small status dict (for tests)."""
        s = self.storage
        try:
            resp = await client.get(
                self.settings.gateway_url,
                timeout=self.settings.http_timeout,
                headers={"Accept": "application/x-protobuf"},
            )
            resp.raise_for_status()
            body = resp.content
        except Exception as exc:  # network error / timeout / non-2xx
            s.record_poll_error(f"拉取失败: {type(exc).__name__}: {exc}")
            return {"outcome": "fetch_error", "error": str(exc)}

        known = s.list_schedule_trip_ids()
        try:
            parsed = parse_snapshot(body, known, s.get_service_date())
        except SnapshotParseError as exc:
            s.record_poll_error(f"解析失败: {exc}")
            return {"outcome": "parse_error", "error": str(exc)}

        last = s.latest_applied_snapshot()
        if last is not None and parsed.header_ts <= last["header_ts"]:
            # Late/duplicate snapshot: keep current results untouched.
            s.record_rejected_snapshot(
                parsed.header_ts,
                parsed.body_hash,
                f"stale header_ts {parsed.header_ts} <= "
                f"{last['header_ts']}",
                parsed.total_entities,
            )
            s.record_poll_attempt_ok()
            return {
                "outcome": "stale_ignored",
                "header_ts": parsed.header_ts,
            }

        version = s.apply_snapshot(
            header_ts=parsed.header_ts,
            body_hash=parsed.body_hash,
            entities=[
                {
                    "trip_id": e.trip_id,
                    "cancelled": e.cancelled,
                    "route_id": e.route_id,
                    "direction_id": e.direction_id,
                    "trip_start_time": e.trip_start_time,
                    "stops": [vars(st) for st in e.stops],
                }
                for e in parsed.entities
            ],
            ignored_count=parsed.ignored_count,
        )
        return {"outcome": "applied", "version": version}

    async def _run(self) -> None:
        async with httpx.AsyncClient(transport=self._transport) as client:
            while not self._stopping.is_set():
                start = time.monotonic()
                try:
                    await self.poll_once(client)
                except Exception as exc:  # defensive: never kill the loop
                    self.storage.record_poll_error(
                        f"轮询异常: {type(exc).__name__}: {exc}"
                    )
                elapsed = time.monotonic() - start
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._stopping.wait(),
                        timeout=max(0.1, self.settings.poll_interval - elapsed),
                    )

    def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None


class Pusher:
    def __init__(self, storage: Storage, settings: Settings, transport=None):
        self.storage = storage
        self.settings = settings
        self._transport = transport
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    # --------------------------------------------------------- reconciliation
    async def reconcile(self, client: httpx.AsyncClient) -> dict:
        """Read what the sign actually serves and reconcile local state."""
        s = self.storage
        try:
            resp = await client.get(
                f"{self.settings.sign_url}/state",
                timeout=self.settings.http_timeout,
            )
        except Exception as exc:
            s.record_sign_observation(
                reachable=False, version=None, content_hash=None,
                detail=f"站牌不可达: {type(exc).__name__}: {exc}",
            )
            return {"reachable": False, "error": str(exc)}

        if resp.status_code == 404:
            # Sign is reachable but has no effective content yet.
            s.record_sign_observation(
                reachable=True, version=None, content_hash=None,
                detail="站牌可达，尚无生效内容",
            )
            return {"reachable": True, "version": None, "content_hash": None}
        if resp.status_code >= 400:
            s.record_sign_observation(
                reachable=False, version=None, content_hash=None,
                detail=f"站牌查询 HTTP {resp.status_code}",
            )
            return {"reachable": False, "error": f"HTTP {resp.status_code}"}

        sign_state = resp.json()
        version = sign_state.get("version")
        content_hash = sign_state.get("content_hash")
        s.record_sign_observation(
            reachable=True,
            version=version,
            content_hash=content_hash,
            detail=f"站牌当前 version={version}",
        )
        return {"reachable": True, "version": version, "content_hash": content_hash}

    # ------------------------------------------------------------- one cycle
    async def push_once(self, client: httpx.AsyncClient) -> dict:
        s = self.storage
        await self.reconcile(client)
        # Manual/loop call: make previously failed attempts eligible again.
        s.requeue_failed()

        attempt = s.claim_next_push()
        if attempt is None:
            return {"outcome": "nothing_to_push"}
        version = attempt["version"]
        # The payload was frozen at claim time: content, hash and receipt
        # describe the same plan + realtime basis by construction.
        payload = json.loads(attempt["payload_json"])

        try:
            resp = await client.post(
                f"{self.settings.sign_url}/state",
                json=payload,
                timeout=self.settings.http_timeout,
                headers={
                    "Idempotency-Key": payload["idempotency_key"],
                    "Content-Hash": payload["content_hash"],
                },
            )
        except Exception as exc:
            # Offline sign / lost response: keep last-good state, retry later.
            s.mark_push_failed(attempt["id"], None,
                               f"推送异常: {type(exc).__name__}: {exc}")
            s.record_sign_observation(False, None, None,
                                      detail=f"推送失败: {exc}")
            return {"outcome": "push_error", "error": str(exc)}

        if resp.status_code in (200, 201, 204):
            # The receipt must correspond to what was sent: a 2xx echoing a
            # different version is not a confirmation of this attempt.
            echoed_version = None
            echoed_hash = None
            with contextlib.suppress(Exception):
                body = resp.json()
                echoed_version = body.get("version")
                echoed_hash = body.get("content_hash")
            if echoed_version is not None and echoed_version != version:
                s.mark_push_failed(
                    attempt["id"], resp.status_code,
                    f"回执版本不符: 发送 v{version} 回执 v{echoed_version}",
                )
                await self.reconcile(client)
                return {"outcome": "receipt_mismatch", "version": version}
            confirmed = s.mark_push_confirmed(attempt["id"], resp.status_code)
            s.record_sign_observation(
                True,
                echoed_version if echoed_version is not None else version,
                echoed_hash or payload["content_hash"],
                detail="推送回执确认",
            )
            if not confirmed:
                # A newer desired state superseded/replaced this attempt
                # while the push was in flight; its own attempt will push.
                return {"outcome": "superseded", "version": version}
            return {"outcome": "confirmed", "version": version}

        if resp.status_code == 409:
            # Sign already serves a different version. Reconcile to learn
            # which one; a newer sign version makes this push obsolete.
            detail = f"409 拒绝旧版本: {resp.text[:200]}"
            await self.reconcile(client)
            observed = s.get_sync_summary()["sign_observed_version"]
            if observed is not None and observed >= version:
                s.mark_push_superseded(attempt["id"], detail)
                return {"outcome": "superseded", "version": version,
                        "sign_version": observed}
            s.mark_push_failed(attempt["id"], 409, detail)
            return {"outcome": "stale_rejected", "version": version}

        s.mark_push_failed(
            attempt["id"], resp.status_code,
            f"HTTP {resp.status_code}: {resp.text[:200]}",
        )
        return {"outcome": "http_error", "status": resp.status_code}

    async def _run(self) -> None:
        async with httpx.AsyncClient(transport=self._transport) as httpx_client:
            while not self._stopping.is_set():
                try:
                    # push_once requeues FAILED attempts and idempotently
                    # retries until the sign confirms the desired version.
                    await self.push_once(httpx_client)
                except Exception as exc:  # defensive
                    self.storage.record_sign_observation(
                        False, None, None,
                        detail=f"推送循环异常: {type(exc).__name__}: {exc}",
                    )
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._stopping.wait(),
                        timeout=max(0.1, self.settings.push_retry_interval),
                    )

    def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
