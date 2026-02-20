#!/usr/bin/env python3
"""Автоматическая подготовка PostgreSQL для CG DB-Writer.

Использование:
  python scripts/setup_db.py --config config.yml

Что делает:
  1) Подключается к PostgreSQL (параметры из config.yml)
  2) Применяет schema/001_init.sql (CREATE TABLE IF NOT EXISTS — безопасно)
  3) Печатает список таблиц и количество строк
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config  # noqa: E402

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schema" / "001_init.sql"


async def main(config_path: str) -> None:
    cfg = load_config(config_path)
    pg = cfg.postgres

    try:
        import asyncpg  # type: ignore
    except ImportError:
        print("ERROR: asyncpg не установлен.\n  pip install asyncpg")
        raise SystemExit(1)

    print(f"Подключение к PostgreSQL {pg.host}:{pg.port}/{pg.dbname} ...")
    try:
        conn = await asyncpg.connect(
            host=pg.host,
            port=pg.port,
            database=pg.dbname,
            user=pg.user,
            password=pg.password,
        )
    except Exception as e:
        print(f"ОШИБКА подключения: {e}")
        print()
        print("Убедитесь что:")
        print(f" 1) PostgreSQL запущен на {pg.host}:{pg.port}")
        print(f" 2) БД '{pg.dbname}' существует")
        print(f" 3) Пользователь '{pg.user}' имеет доступ")
        print()
        print("Создать БД и пользователя вручную:")
        print(" sudo -u postgres psql")
        print(f" CREATE USER {pg.user} WITH PASSWORD '...';")
        print(f" CREATE DATABASE {pg.dbname} OWNER {pg.user};")
        raise SystemExit(1)

    print("Подключено!")

    if not SCHEMA_FILE.exists():
        print(f"ОШИБКА: файл схемы не найден: {SCHEMA_FILE}")
        await conn.close()
        raise SystemExit(1)

    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    print(f"Применяю схему из {SCHEMA_FILE.name} ...")
    try:
        await conn.execute(sql)
    except Exception as e:
        print(f"ОШИБКА при применении схемы: {e}")
        await conn.close()
        raise SystemExit(1)

    rows = await conn.fetch(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
        """
    )
    print()
    print("Таблицы в БД:")
    for r in rows:
        tablename = r["tablename"]
        count = await conn.fetchval(f'SELECT count(*) FROM "{tablename}"')
        print(f" {tablename:30s} ({count} строк)")

    await conn.close()
    print()
    print("Готово! Схема применена успешно.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Setup DB for CG DB-Writer")
    p.add_argument("-c", "--config", default="config.yml", help="Путь к config.yml (default: config.yml)")
    args = p.parse_args()
    asyncio.run(main(args.config))
