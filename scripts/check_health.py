#!/usr/bin/env python3
"""CG DB-Writer v2.1.0 — проверка здоровья системы.

Использование:
  python scripts/check_health.py --config config.yml

Проверяет:
  1. PostgreSQL — подключение, таблицы, Continuous Aggregates, свежесть данных
  2. MQTT — подключение
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
    "state_events",
    "parameter_history",
    "events",
]

EXPECTED_VIEWS = [
    "history_1min",
    "history_1hour",
]


async def check_postgres(cfg) -> bool:
    pg = cfg.postgres
    import asyncpg

    print("=" * 60)
    print("PostgreSQL / TimescaleDB")
    print("=" * 60)
    print(f"  host: {pg.host}:{pg.port}  db: {pg.dbname}  user: {pg.user}")

    try:
        conn = await asyncpg.connect(
            host=pg.host, port=pg.port,
            database=pg.dbname,
            user=pg.user, password=pg.password,
        )
    except Exception as e:
        print(f"  ОШИБКА подключения: {e}")
        return False

    print("  Подключение: OK")

    # TimescaleDB
    ts_ver = await conn.fetchval(
        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
    )
    if ts_ver:
        print(f"  TimescaleDB: {ts_ver}")
    else:
        print("  TimescaleDB: НЕ УСТАНОВЛЕН — запустите setup_db.py")
        await conn.close()
        return False

    # Таблицы
    existing_tables = {
        r["tablename"]
        for r in await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    }
    existing_views = {
        r["matviewname"]
        for r in await conn.fetch("SELECT matviewname FROM pg_matviews WHERE schemaname = 'public'")
    }

    missing_t = [t for t in EXPECTED_TABLES if t not in existing_tables]
    missing_v = [v for v in EXPECTED_VIEWS  if v not in existing_views]

    if missing_t or missing_v:
        if missing_t:
            print(f"  ОТСУТСТВУЮТ таблицы: {', '.join(missing_t)}")
        if missing_v:
            print(f"  ОТСУТСТВУЮТ вью: {', '.join(missing_v)}")
        print("  Запустите: python scripts/setup_db.py --config config.yml")
        await conn.close()
        return False

    print("  Таблицы и CA: все на месте")

    # Данные
    print()
    print("  Данные:")
    checks = [
        ("history",          "SELECT count(*), max(received_at) FROM history"),
        ("history_1min",     "SELECT count(*) FROM history_1min"),
        ("history_1hour",    "SELECT count(*) FROM history_1hour"),
        ("state_events",     "SELECT count(*), max(received_at) FROM state_events"),
        ("parameter_history","SELECT count(*), max(received_at) FROM parameter_history"),
        ("gps_raw_history",  "SELECT count(*), max(received_at) FROM gps_raw_history"),
        ("latest_state",     "SELECT count(*), max(updated_at)  FROM latest_state"),
        ("events",           "SELECT count(*), max(created_at)  FROM events"),
        ("objects",          "SELECT count(*) FROM objects"),
    ]

    for name, sql in checks:
        try:
            row = await conn.fetchrow(sql)
            count = row[0]
            ts    = row[1] if len(row) > 1 else None
            suffix = ""
            if ts:
                age = (datetime.now(timezone.utc) - ts).total_seconds()
                suffix = f"  (последняя {age:.0f}s назад)"
            print(f"    {name:20s} {count:>10,}{suffix}")
        except Exception as e:
            print(f"    {name:20s}  ERROR: {e}")

    # Retention jobs
    jobs = await conn.fetch(
        """
        SELECT application_name, next_start, last_run_status
        FROM timescaledb_information.jobs
        WHERE application_name LIKE '%Retention%'
           OR application_name LIKE '%Compression%'
           OR application_name LIKE '%Continuous%'
        ORDER BY application_name
        """
    )
    if jobs:
        print()
        print("  TimescaleDB jobs:")
        for j in jobs:
            print(f"    {j['application_name'][:40]:40s}  next={j['next_start']}  last={j['last_run_status']}")

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
        ):
            pass
        print("  Подключение: OK")
        return True
    except Exception as e:
        print(f"  ОШИБКА: {e}")
        return False


async def main_async(config_path: str) -> None:
    cfg = load_config(config_path)
    pg_ok   = await check_postgres(cfg)
    mqtt_ok = await check_mqtt(cfg)

    print()
    print("=" * 60)
    print("ИТОГО")
    print("=" * 60)
    print(f"  PostgreSQL/TimescaleDB: {'OK' if pg_ok   else 'ОШИБКА'}")
    print(f"  MQTT:                   {'OK' if mqtt_ok else 'ОШИБКА'}")

    if pg_ok and mqtt_ok:
        print("\n  Всё в порядке! DB-Writer v2.1.0 готов к работе.")
        return

    print("\n  Есть проблемы. Исправьте ошибки выше.")
    raise SystemExit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="CG DB-Writer v2.1.0 health check")
    p.add_argument("-c", "--config", default="config.yml")
    args = p.parse_args()
    asyncio.run(main_async(args.config))
