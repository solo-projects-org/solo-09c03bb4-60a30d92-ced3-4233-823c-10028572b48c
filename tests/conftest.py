"""Pytest fixtures: run all three ASGI apps in one process.

The webapp's outbound httpx calls are redirected to in-process ASGI
transports keyed by virtual host (gateway / sign), so no real network or
Docker daemon is needed for the interleaving tests.
"""
from __future__ import annotations

import asyncio
import os
import tempfile

import httpx
import pytest

# Settings must be configured before the webapp modules are imported.
_TMP = tempfile.mkdtemp(prefix="transit-test-")
os.environ["APP_DB_PATH"] = os.path.join(_TMP, "webapp.db")
os.environ["GATEWAY_URL"] = "http://gateway/gtfs-rt"
os.environ["SIGN_URL"] = "http://sign"
os.environ["BACKGROUND_WORKERS"] = "0"
os.environ["HTTP_TIMEOUT"] = "1.5"
os.environ["POLL_INTERVAL"] = "0.05"
os.environ["PUSH_RETRY_INTERVAL"] = "0.05"
os.environ["SIGN_DB_PATH"] = os.path.join(_TMP, "sign.db")

from gateway import app as gateway_app  # noqa: E402
from sign import app as sign_app  # noqa: E402
from webapp import main as webmain  # noqa: E402
from webapp.config import settings  # noqa: E402
from webapp.storage import Storage  # noqa: E402
from webapp.sync import Poller, Pusher  # noqa: E402


class MultiASGITransport(httpx.AsyncBaseTransport):
    def __init__(self, apps: dict[str, httpx.ASGITransport]):
        self._apps = apps
        self.sign_offline = False
        # Simulated network latency per host (seconds), enforced in the
        # transport so httpx timeouts fire; in-process ASGI sleeps do not.
        self.latency: dict[str, float] = {}
        # Hang (delay past timeout) requests whose (host, method, path prefix)
        # matches. Two flavors:
        #   hang_rules        -> delay BEFORE dispatch (request never arrives)
        #   hang_after_rules  -> run the ASGI app (so it commits), then lose
        #                        the response past the read timeout
        self.hang_rules: list[tuple[str, str, str]] = []
        self.hang_after_rules: list[tuple[str, str, str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "sign" and self.sign_offline:
            raise httpx.ConnectError("simulated sign outage")
        # Custom httpx transports do not enforce the client timeout
        # themselves; emulate it so the app's timeout handling is exercised.
        timeout = request.extensions.get("timeout", {})
        deadline = timeout.get("read") if timeout else None
        latency = self.latency.get(host, 0.0)
        hang_before = any(
            host == h and request.method == m and str(request.url.path).startswith(p)
            for h, m, p in self.hang_rules
        )
        if hang_before:
            latency = max(latency, 5.0)
        if latency:
            if deadline is not None and latency >= deadline:
                await asyncio.sleep(deadline)
                raise httpx.ReadTimeout(f"simulated timeout after {deadline}s")
            await asyncio.sleep(latency)
        transport = self._apps.get(host)
        if transport is None:
            raise httpx.ConnectError(f"no test app for host {host}")
        response = await transport.handle_async_request(request)
        hang_after = any(
            host == h and request.method == m and str(request.url.path).startswith(p)
            for h, m, p in self.hang_after_rules
        )
        if hang_after and deadline is not None:
            # Server side already committed; the client gives up waiting.
            await asyncio.sleep(deadline)
            raise httpx.ReadTimeout("response lost after server commit")
        return response


@pytest.fixture
def transport():
    apps = {
        "gateway": httpx.ASGITransport(app=gateway_app.app),
        "sign": httpx.ASGITransport(app=sign_app.app),
    }
    return MultiASGITransport(apps)


@pytest.fixture(autouse=True)
async def _isolate_services():
    """Reset the in-process gateway and sign state between every test."""
    gateway_app.STATE["delay"] = 0.0
    gateway_app.STATE["failure"] = "NONE"
    gateway_app.STATE["entities"] = []
    import time as _time

    gateway_app.STATE["next_header_ts"] = int(_time.time())
    db = sign_app._db()
    db.execute("DELETE FROM sign_state")
    db.execute("DELETE FROM sign_history")
    db.commit()
    sign_app._RL["mode"] = "OFF"
    sign_app._RL["idem_keys"].clear()
    yield


@pytest.fixture
def storage():
    import uuid

    db = os.path.join(_TMP, f"webapp-{uuid.uuid4().hex}.db")
    s = Storage(db)
    s.reset_inflight_on_startup()
    yield s
    s.close()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db + suffix)
        except OSError:
            pass


@pytest.fixture
def wired(storage, transport):
    """Wire the webapp globals; outbound HTTP is injected via transports."""
    webmain._STORAGE = storage
    poller = Poller(storage, settings, transport=transport)
    pusher = Pusher(storage, settings, transport=transport)
    webmain._POLLER = poller
    webmain._PUSHER = pusher

    return {
        "storage": storage,
        "poller": poller,
        "pusher": pusher,
        "transport": transport,
        "web_client": httpx.AsyncClient(
            transport=httpx.ASGITransport(app=webmain.app),
            base_url="http://webapp",
        ),
        "gateway": httpx.AsyncClient(
            transport=transport, base_url="http://gateway"
        ),
        "sign": httpx.AsyncClient(transport=transport, base_url="http://sign"),
    }


SAMPLE_CSV = """trip_id,stop_sequence,stop_code,scheduled_time
T1,1,ST-A,08:00:00
T1,2,ST-B,08:05:00
T1,3,ST-C,08:10:00
T2,1,ST-A,09:00:00
T2,2,ST-B,09:05:00
"""
