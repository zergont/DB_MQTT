"""
CG DB-Writer — фоновая задача мониторинга online/offline + stale.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from src import db
from src.config import AppConfig

logger = logging.getLogger("cg.watchdog")


# Состояние конечного автомата per-entity
# Возможные состояния: "online", "stale", "offline"
_router_state: dict[str, str] = {}
_panel_state: dict[tuple[str, int], str] = {}


async def watchdog_loop(
    cfg: AppConfig,
    last_seen: dict[str, datetime],
    panel_last_seen: dict[tuple[str, int], datetime],
) -> None:
    """Бесконечный цикл проверки stale/offline."""
    interval = cfg.events_policy.check_interval_sec
    logger.info("Watchdog started, interval=%ds", interval)

    while True:
        await asyncio.sleep(interval)
        try:
            await _check(cfg, last_seen, panel_last_seen)
        except Exception:
            logger.exception("Watchdog check error")


async def _check(
    cfg: AppConfig,
    last_seen: dict[str, datetime],
    panel_last_seen: dict[tuple[str, int], datetime],
) -> None:
    now = datetime.now(timezone.utc)
    ep = cfg.events_policy

    # --- Routers ---
    for router_sn, ts in list(last_seen.items()):
        age = (now - ts).total_seconds()
        prev = _router_state.get(router_sn, "online")

        if age >= ep.router_offline_sec:
            new_state = "offline"
        elif age >= ep.router_stale_sec:
            new_state = "stale"
        else:
            new_state = "online"

        if new_state != prev:
            await _emit_router_event(router_sn, prev, new_state)
            _router_state[router_sn] = new_state

    # --- Panels ---
    for (router_sn, panel_id), ts in list(panel_last_seen.items()):
        age = (now - ts).total_seconds()
        prev = _panel_state.get((router_sn, panel_id), "online")

        if age >= ep.panel_offline_sec:
            new_state = "offline"
        elif age >= ep.panel_stale_sec:
            new_state = "stale"
        else:
            new_state = "online"

        if new_state != prev:
            await _emit_panel_event(router_sn, panel_id, prev, new_state)
            _panel_state[(router_sn, panel_id)] = new_state


async def _emit_router_event(router_sn: str, prev: str, new: str) -> None:
    if new == "offline":
        event_type = "router_offline"
    elif new == "online" and prev in ("offline", "stale"):
        event_type = "router_online"
    else:
        return  # stale — не пишем отдельного event, только offline/online

    logger.info("Router %s: %s → %s", router_sn, prev, new)
    async with db.pool().acquire() as conn:
        await db.insert_event(
            conn, router_sn,
            event_type,
            description=f"{prev} → {new}",
        )


async def _emit_panel_event(
    router_sn: str, panel_id: int, prev: str, new: str,
) -> None:
    if new == "offline":
        event_type = "panel_offline"
    elif new == "online" and prev in ("offline", "stale"):
        event_type = "panel_online"
    else:
        return

    logger.info("Panel %s/pcc/%d: %s → %s", router_sn, panel_id, prev, new)
    async with db.pool().acquire() as conn:
        await db.insert_event(
            conn, router_sn,
            event_type,
            description=f"panel_id={panel_id} {prev} → {new}",
            equip_type="pcc",
            panel_id=panel_id,
        )
