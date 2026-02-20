"""
CG DB-Writer — фоновая задача очистки устаревших данных (retention).
"""

from __future__ import annotations

import asyncio
import logging

from src import db
from src.config import RetentionCfg

logger = logging.getLogger("cg.retention")


async def retention_loop(cfg: RetentionCfg) -> None:
    """Запускает очистку с интервалом cfg.cleanup_interval_hours.

    Первая очистка выполняется сразу при старте, чтобы после рестарта не ждать весь интервал.
    """
    interval_sec = cfg.cleanup_interval_hours * 3600
    logger.info(
        "Retention task started: interval=%dh, gps_raw=%dh, history=%dd, events=%dd",
        cfg.cleanup_interval_hours,
        cfg.gps_raw_hours,
        cfg.history_days,
        cfg.events_days,
    )

    while True:
        try:
            await _do_cleanup(cfg)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Retention cleanup error")

        await asyncio.sleep(interval_sec)


async def _do_cleanup(cfg: RetentionCfg) -> None:
    logger.info("Retention cleanup started")
    async with db.pool().acquire() as conn:
        n1 = await db.cleanup_gps_raw(conn, cfg.gps_raw_hours, cfg.batch_size)
        if n1:
            logger.info("  gps_raw_history: deleted %d rows", n1)

        n2 = await db.cleanup_history(conn, cfg.history_days, cfg.batch_size)
        if n2:
            logger.info("  history: deleted %d rows", n2)

        n3 = await db.cleanup_events(conn, cfg.events_days, cfg.batch_size)
        if n3:
            logger.info("  events: deleted %d rows", n3)

    logger.info("Retention cleanup done (gps=%d, hist=%d, evt=%d)", n1, n2, n3)
