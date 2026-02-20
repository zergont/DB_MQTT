#!/usr/bin/env python3
"""Проверка здоровья CG DB-Writer: подключения, таблицы, данные.

Использование:
  python scripts/check_health.py --config config.yml

Проверяет:
  1) PostgreSQL — подключение + наличие таблиц + счётчики строк
  2) MQTT — подключение (быстрая попытка)
  3) Свежесть данных — когда последний раз что-то записывалось
"""

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config  # noqa: E402

EXPECTED_TABLES = [
    "objects",
    "equipment",
    "register_catalog",
    "gps_raw_history",
    "gps_latest_filtered",
    "latest_state",
    "history",
    "events",
]


async def check_postgres(cfg) -> bool:
    pg = cfg.postgres
    import asyncpg  # type: ignore

    print("=" * 60)
    print("PostgreSQL")
    print("=" * 60)
    print(f" host: {pg.host}:{pg.port} db: {pg.dbname} user: {pg.user}")

    try:
        conn = await asyncpg.connect(
            host=pg.host,
            port=pg.port,
            database=pg.dbname,
            user=pg.user,
            password=pg.password,
        )
    except Exception as e:
        print(f" ОШИБКА: {e}")
        return False

    print(" Подключение: OK")

    rows = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    existing = {r["tablename"] for r in rows}
    missing = [t for t in EXPECTED_TABLES if t not in existing]
    if missing:
        print(f" ОТСУТСТВУЮТ таблицы: {', '.join(missing)}")
        print(" Запустите: python scripts/setup_db.py --config config.yml")
        await conn.close()
        return False

    print(" Таблицы: все на месте")
    print()
    print(" Данные в таблицах:")

    for t in EXPECTED_TABLES:
        count = await conn.fetchval(f'SELECT count(*) FROM "{t}"')
        suffix = ""

        if t == "gps_raw_history" and count > 0:
            last = await conn.fetchval("SELECT max(received_at) FROM gps_raw_history")
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f" (последняя запись {age:.0f}s назад)"
        elif t == "latest_state" and count > 0:
            last = await conn.fetchval("SELECT max(updated_at) FROM latest_state")
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f" (обновлено {age:.0f}s назад)"
        elif t == "history" and count > 0:
            last = await conn.fetchval("SELECT max(received_at) FROM history")
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f" (последняя {age:.0f}s назад)"
        elif t == "events" and count > 0:
            last = await conn.fetchval("SELECT max(created_at) FROM events")
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f" (последнее {age:.0f}s назад)"

        print(f" {t:30s} {count:>8d}{suffix}")

    await conn.close()
    return True


async def check_mqtt(cfg) -> bool:
    mc = cfg.mqtt
    print()
    print("=" * 60)
    print("MQTT")
    print("=" * 60)
    print(f" host: {mc.host}:{mc.port} user: {mc.user or '(anonymous)'}")

    try:
        import aiomqtt  # type: ignore
    except ImportError:
        print(" ПРОПУЩЕНО: aiomqtt не установлен")
        return False

    try:
        async with aiomqtt.Client(
            hostname=mc.host,
            port=mc.port,
            username=mc.user or None,
            password=mc.password or None,
            identifier="cg-health-check",
            keepalive=5,
            tls_params=aiomqtt.TLSParameters() if mc.tls else None,
        ):
            pass
        print(" Подключение: OK")
        return True
    except Exception as e:
        print(f" ОШИБКА: {e}")
        return False


async def main(config_path: str) -> None:
    cfg = load_config(config_path)
    print()
    pg_ok = await check_postgres(cfg)
    mqtt_ok = await check_mqtt(cfg)

    print()
    print("=" * 60)
    print("ИТОГО")
    print("=" * 60)
    print(f" PostgreSQL: {'OK' if pg_ok else 'ОШИБКА'}")
    print(f" MQTT: {'OK' if mqtt_ok else 'ОШИБКА'}")

    if pg_ok and mqtt_ok:
        print()
        print(" Всё в порядке! DB-Writer может работать.")
        return

    print()
    print(" Есть проблемы.\n Исправьте ошибки выше.")
    raise SystemExit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="CG DB-Writer health check")
    p.add_argument("-c", "--config", default="config.yml", help="Путь к config.yml (default: config.yml)")
    args = p.parse_args()
    asyncio.run(main(args.config))
