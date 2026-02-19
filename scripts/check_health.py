#!/usr/bin/env python3
"""
Проверка здоровья CG DB-Writer: подключения, таблицы, данные.

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

from src.config import load_config

EXPECTED_TABLES = [
    "objects", "equipment", "register_catalog",
    "gps_raw_history", "gps_latest_filtered",
    "latest_state", "history", "events",
]


async def check_postgres(cfg) -> bool:
    pg = cfg.postgres
    import asyncpg

    print("=" * 60)
    print("PostgreSQL")
    print("=" * 60)
    print(f"  host: {pg.host}:{pg.port}  db: {pg.dbname}  user: {pg.user}")

    try:
        conn = await asyncpg.connect(
            host=pg.host, port=pg.port,
            database=pg.dbname, user=pg.user, password=pg.password,
        )
    except Exception as e:
        print(f"  ОШИБКА: {e}")
        return False

    print("  Подключение: OK")

    # Таблицы
    rows = await conn.fetch("""
        SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    """)
    existing = {r["tablename"] for r in rows}
    missing = [t for t in EXPECTED_TABLES if t not in existing]
    if missing:
        print(f"  ОТСУТСТВУЮТ таблицы: {', '.join(missing)}")
        print("  Запустите: python scripts/setup_db.py --config config.yml")
        await conn.close()
        return False

    print("  Таблицы: все на месте")

    # Счётчики
    print()
    print("  Данные в таблицах:")
    for t in EXPECTED_TABLES:
        count = await conn.fetchval(f'SELECT count(*) FROM "{t}"')
        suffix = ""
        # Для некоторых таблиц показываем свежесть
        if t == "gps_raw_history" and count > 0:
            last = await conn.fetchval(
                "SELECT max(received_at) FROM gps_raw_history"
            )
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f"  (последняя запись {age:.0f}s назад)"
        elif t == "latest_state" and count > 0:
            last = await conn.fetchval(
                "SELECT max(updated_at) FROM latest_state"
            )
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f"  (обновлено {age:.0f}s назад)"
        elif t == "history" and count > 0:
            last = await conn.fetchval(
                "SELECT max(received_at) FROM history"
            )
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f"  (последняя {age:.0f}s назад)"
        elif t == "events" and count > 0:
            last = await conn.fetchval(
                "SELECT max(created_at) FROM events"
            )
            if last:
                age = (datetime.now(timezone.utc) - last).total_seconds()
                suffix = f"  (последнее {age:.0f}s назад)"

        print(f"    {t:30s}  {count:>8d}{suffix}")

    # Объекты
    objects = await conn.fetch("SELECT router_sn, updated_at FROM objects ORDER BY updated_at DESC LIMIT 5")
    if objects:
        print()
        print("  Последние объекты:")
        for o in objects:
            print(f"    {o['router_sn']}  (обновлён {o['updated_at']})")

    # GPS latest
    gps_rows = await conn.fetch("SELECT router_sn, lat, lon, received_at FROM gps_latest_filtered ORDER BY received_at DESC LIMIT 5")
    if gps_rows:
        print()
        print("  GPS последние фильтрованные:")
        for g in gps_rows:
            print(f"    {g['router_sn']}  lat={g['lat']:.6f} lon={g['lon']:.6f}  ({g['received_at']})")

    await conn.close()
    return True


async def check_mqtt(cfg) -> bool:
    mc = cfg.mqtt
    print()
    print("=" * 60)
    print("MQTT")
    print("=" * 60)
    print(f"  host: {mc.host}:{mc.port}  user: {mc.user or '(anonymous)'}")

    try:
        import aiomqtt
    except ImportError:
        print("  ПРОПУЩЕНО: aiomqtt не установлен")
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
        ) as client:
            # Если подключились — OK
            pass
        print("  Подключение: OK")
        return True
    except Exception as e:
        print(f"  ОШИБКА: {e}")
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
    print(f"  PostgreSQL:  {'OK' if pg_ok else 'ОШИБКА'}")
    print(f"  MQTT:        {'OK' if mqtt_ok else 'ОШИБКА'}")

    if pg_ok and mqtt_ok:
        print()
        print("  Всё в порядке! DB-Writer может работать.")
    else:
        print()
        print("  Есть проблемы. Исправьте ошибки выше.")
        sys.exit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="CG DB-Writer health check")
    p.add_argument("-c", "--config", default="config.yml",
                   help="Путь к config.yml (default: config.yml)")
    args = p.parse_args()
    asyncio.run(main(args.config))
