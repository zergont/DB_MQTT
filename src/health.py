"""HTTP health endpoint для DB-Writer."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from aiohttp import web

from src import db
from src.config import HealthCfg
from src.version import get_version

logger = logging.getLogger("cg.health")


@dataclass(slots=True)
class HealthState:
    q_decoded: asyncio.Queue[Any] | None = None
    q_telemetry: asyncio.Queue[Any] | None = None
    worker_tasks: tuple[asyncio.Task[Any], ...] = field(default_factory=tuple)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_write_at: datetime | None = None

    def mark_write(self) -> None:
        self.last_write_at = datetime.now(timezone.utc)


def _age_sec(ts: datetime) -> float:
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _pool_size() -> int:
    try:
        return int(db.pool().get_size())
    except Exception:
        return 0


def _workers_alive(state: HealthState) -> int:
    return sum(1 for t in state.worker_tasks if not t.done() and not t.cancelled())


def _payload(state: HealthState) -> dict[str, Any]:
    last_write_ago_sec = _age_sec(state.last_write_at or state.started_at)
    workers_alive = _workers_alive(state)

    if workers_alive <= 0 or last_write_ago_sec >= 300:
        status = "dead"
    elif last_write_ago_sec >= 60:
        status = "degraded"
    else:
        status = "ok"

    return {
        "version": get_version(),
        "status": status,
        "queue_decoded_size": state.q_decoded.qsize() if state.q_decoded else 0,
        "queue_telemetry_size": state.q_telemetry.qsize() if state.q_telemetry else 0,
        "last_write_ago_sec": round(last_write_ago_sec, 1),
        "workers_alive": workers_alive,
        "db_pool_size": _pool_size(),
    }


async def _handle_health(request: web.Request) -> web.Response:
    state: HealthState = request.app["state"]
    return web.json_response(_payload(state))


async def health_loop(cfg: HealthCfg, state: HealthState) -> None:
    app = web.Application()
    app["state"] = state
    app.router.add_get("/health", _handle_health)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, cfg.bind, cfg.port)
    await site.start()
    logger.info("Health endpoint started on http://%s:%d/health", cfg.bind, cfg.port)

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Health endpoint stopped")
        raise
    finally:
        await runner.cleanup()
