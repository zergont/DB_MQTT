"""
CG DB-Writer — фоновая агрегация history → history_1min → history_1hour.

Запускается как asyncio task. Каждую минуту агрегирует предыдущую завершённую
минуту. При пересечении границы часа — также агрегирует часовые данные.

Dirty-tracking: если опоздавшая запись попала в уже агрегированную минуту,
эта минута помечается как dirty и переагрегируется в следующем цикле.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from src import db

logger = logging.getLogger("cg.aggregation")

# ── Module-level state ──────────────────────────────────────────────────────

_last_aggregated_minute: datetime | None = None
_dirty_minutes: set[datetime] = set()


def get_aggregation_watermark() -> datetime | None:
    """Последняя успешно агрегированная минута (для retention safety)."""
    return _last_aggregated_minute


def notify_history_write(ts: datetime | None) -> None:
    """Вызывается после коммита записи в history.

    Если ts попадает в уже агрегированную минуту — помечаем dirty.
    """
    if ts is None or _last_aggregated_minute is None:
        return
    minute = ts.replace(second=0, microsecond=0)
    if minute <= _last_aggregated_minute:
        _dirty_minutes.add(minute)
        logger.debug("Marked minute %s as dirty (late write)", minute)


# ── Main loop ───────────────────────────────────────────────────────────────

async def aggregation_loop() -> None:
    """Фоновая задача агрегации (asyncio task)."""
    global _last_aggregated_minute

    logger.info("Aggregation task started")

    while True:
        try:
            now = datetime.now(timezone.utc)
            # Предыдущая завершённая минута
            current_minute = now.replace(second=0, microsecond=0) - timedelta(minutes=1)

            minutes_to_aggregate: list[datetime] = [current_minute]

            # Забираем dirty-минуты (атомарно в asyncio — однопоточно)
            dirty_copy = _dirty_minutes.copy()
            _dirty_minutes.clear()
            minutes_to_aggregate.extend(dirty_copy)

            # Дедупликация и сортировка
            minutes_to_aggregate = sorted(set(minutes_to_aggregate))

            # Часы, которые нужно переагрегировать (из dirty-минут прошлых часов)
            current_hour = current_minute.replace(minute=0, second=0, microsecond=0)
            hours_to_reaggregate: set[datetime] = set()

            async with db.pool().acquire() as conn:
                for minute_start in minutes_to_aggregate:
                    minute_end = minute_start + timedelta(minutes=1)
                    n = await db.aggregate_to_1min(conn, minute_start, minute_end)
                    if n:
                        logger.debug("Aggregated 1min %s: %d groups", minute_start, n)

                    # Dirty-минута из прошлого часа → нужно переагрегировать час
                    minute_hour = minute_start.replace(minute=0, second=0, microsecond=0)
                    if minute_start in dirty_copy and minute_hour < current_hour:
                        hours_to_reaggregate.add(minute_hour)

                # Обновляем watermark
                _last_aggregated_minute = current_minute

                # Часовая агрегация: текущий час при пересечении границы
                if current_minute.minute == 59:
                    hour_start = current_hour
                    hour_end = hour_start + timedelta(hours=1)
                    n = await db.aggregate_to_1hour(conn, hour_start, hour_end)
                    if n:
                        logger.info("Aggregated 1hour %s: %d groups", hour_start, n)

                # Переагрегация прошлых часов (dirty)
                for hour_start in sorted(hours_to_reaggregate):
                    hour_end = hour_start + timedelta(hours=1)
                    n = await db.aggregate_to_1hour(conn, hour_start, hour_end)
                    if n:
                        logger.info("Re-aggregated 1hour %s (dirty): %d groups", hour_start, n)

            if dirty_copy:
                logger.info(
                    "Aggregation cycle: %d minute(s), %d dirty, %d hour(s) re-aggregated",
                    len(minutes_to_aggregate),
                    len(dirty_copy),
                    len(hours_to_reaggregate),
                )

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Aggregation error")

        await asyncio.sleep(60)
