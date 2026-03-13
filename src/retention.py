"""
CG DB-Writer — фоновая задача очистки устаревших данных (retention).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from src import db
from src.aggregation import get_aggregation_watermark
from src.config import RetentionCfg

logger = logging.getLogger("cg.retention")


async def retention_loop(cfg: RetentionCfg) -> None:
    """Запускает очистку с интервалом cfg.cleanup_interval_hours.

    Первая очистка выполняется сразу при старте, чтобы после рестарта не ждать весь интервал.
    """
    interval_sec = cfg.cleanup_interval_hours * 3600
    logger.info(
        "Retention task started: interval=%dh, raw=%dd, 1min=%dd, 1hour=%dd, gps=%dh, events=%dd",
        cfg.cleanup_interval_hours,
        cfg.history_raw_days,
        cfg.history_1min_days,
        cfg.history_1hour_days,
        cfg.gps_raw_hours,
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

    # Watermark protection: не удаляем raw данные, которые ещё не агрегированы
    now = datetime.now(timezone.utc)
    raw_cutoff = now - timedelta(days=cfg.history_raw_days)
    watermark = get_aggregation_watermark()
    if watermark is not None and watermark < raw_cutoff:
        logger.warning(
            "Aggregation watermark (%s) is behind raw cutoff (%s); using watermark as safe cutoff",
            watermark,
            raw_cutoff,
        )
        raw_cutoff = watermark

    async with db.pool().acquire() as conn:
        n1 = await db.cleanup_gps_raw(conn, cfg.gps_raw_hours, cfg.batch_size)
        if n1:
            logger.info("  gps_raw_history: deleted %d rows", n1)

        n2 = await db.cleanup_history_raw(conn, raw_cutoff, cfg.batch_size)
        if n2:
            logger.info("  history (raw): deleted %d rows", n2)

        n3 = await db.cleanup_history_1min(conn, cfg.history_1min_days, cfg.batch_size)
        if n3:
            logger.info("  history_1min: deleted %d rows", n3)

        n4 = await db.cleanup_history_1hour(conn, cfg.history_1hour_days, cfg.batch_size)
        if n4:
            logger.info("  history_1hour: deleted %d rows", n4)

        n5 = await db.cleanup_events(conn, cfg.events_days, cfg.batch_size)
        if n5:
            logger.info("  events: deleted %d rows", n5)

    logger.info(
        "Retention cleanup done (gps=%d, raw=%d, 1min=%d, 1hour=%d, evt=%d)",
        n1, n2, n3, n4, n5,
    )
