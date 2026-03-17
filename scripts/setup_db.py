#!/usr/bin/env python3
"""CG DB-Writer v2.0.0 — подготовка PostgreSQL/TimescaleDB.

Использование:
  python scripts/setup_db.py --config config.yml [--drop]

  --drop   Сбросить и пересоздать БД с нуля (осторожно!)

Выполняет:
  1. CREATE EXTENSION timescaledb
  2. Создание таблиц, hypertables, Continuous Aggregates
  3. Compression и Retention policies
  4. Роли и права доступа (cg_writer, cg_ui)
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config   # noqa: E402

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schema" / "schema.sql"

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
    "share_links",
]

EXPECTED_VIEWS = [
    "history_1min",
    "history_1hour",
]



def _split_sql(sql: str) -> list[str]:
    """Split SQL into individual statements, respecting dollar-quoted blocks ($$...$$)."""
    statements: list[str] = []
    current: list[str] = []
    dollar_tag: str | None = None   # активный dollar-quote тег, напр. $$ или $func$
    i = 0
    n = len(sql)

    while i < n:
        # Начало или конец dollar-quoted блока
        if dollar_tag is None and sql[i] == "$":
            # Ищем закрывающий $
            j = sql.find("$", i + 1)
            if j != -1:
                tag = sql[i : j + 1]
                dollar_tag = tag
                current.append(sql[i : j + 1])
                i = j + 1
                continue
        elif dollar_tag is not None:
            end = sql.find(dollar_tag, i)
            if end != -1:
                current.append(sql[i : end + len(dollar_tag)])
                i = end + len(dollar_tag)
                dollar_tag = None
                continue

        ch = sql[i]

        # Разделитель команд — только вне dollar-quoted блоков
        if ch == ";" and dollar_tag is None:
            stmt = "".join(current).strip()
            # Пропускаем пустые и чисто-комментарийные «команды»
            lines = [l.strip() for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
            if lines:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    # Хвост без финальной точки с запятой
    stmt = "".join(current).strip()
    lines = [l.strip() for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
    if lines:
        statements.append(stmt)

    return statements


async def setup(cfg, drop: bool) -> None:
    import asyncpg

    pg = cfg.postgres
    print(f"Connecting to PostgreSQL {pg.host}:{pg.port} db={pg.dbname} user={pg.user}")

    conn: asyncpg.Connection = await asyncpg.connect(
        host=pg.host, port=pg.port,
        database=pg.dbname,
        user=pg.user, password=pg.password,
    )

    try:
        # Проверяем что TimescaleDB доступен
        ts_ver = await conn.fetchval(
            "SELECT default_version FROM pg_available_extensions WHERE name = 'timescaledb'"
        )
        if ts_ver is None:
            print("ОШИБКА: TimescaleDB не установлен на сервере PostgreSQL.")
            print("Установите: https://docs.timescale.com/self-hosted/latest/install/")
            sys.exit(1)
        print(f"TimescaleDB доступен: version {ts_ver}")

        if drop:
            confirm = input(
                "\nВНИМАНИЕ: будут удалены ВСЕ таблицы и данные!\n"
                "Введите 'yes' для подтверждения: "
            )
            if confirm.strip().lower() != "yes":
                print("Отменено.")
                return

            # Удаляем в правильном порядке (зависимости)
            print("Dropping existing objects…")
            for view in ("history_1hour", "history_1min"):
                await conn.execute(
                    f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE"
                )
            for tbl in (
                "history", "gps_raw_history", "gps_latest_filtered",
                "latest_state", "state_events", "parameter_history",
                "events", "share_links",
                "equipment", "register_catalog", "objects",
            ):
                await conn.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
            print("Done.")

        # Читаем и применяем схему
        if not SCHEMA_FILE.exists():
            print(f"ОШИБКА: schema file not found: {SCHEMA_FILE}")
            sys.exit(1)

        sql = SCHEMA_FILE.read_text(encoding="utf-8")

        # Выполняем каждую команду отдельно (требование для некоторых TimescaleDB DDL).
        # Используем умный splitter: учитываем dollar-quoted блоки $$...$$ и строки.
        statements = _split_sql(sql)
        total = len(statements)
        print(f"\nApplying schema ({total} statements)…")

        ok = 0
        for i, stmt in enumerate(statements, 1):
            try:
                await conn.execute(stmt)
                ok += 1
            except Exception as e:
                err = str(e)
                # Пропускаем "already exists" ошибки (идемпотентность)
                if "already exists" in err or "DuplicateObject" in str(type(e)):
                    ok += 1
                else:
                    print(f"  [{i}/{total}] WARNING: {err[:120]}")

        print(f"Schema applied: {ok}/{total} statements OK")

        # Создаём роли (если не существуют)
        await _ensure_role(conn, "cg_writer", pg.password)
        await _ensure_role(conn, "cg_ui", "cg_ui_pass")

        # Проверяем результат
        print("\nVerifying tables…")
        existing_tables = {
            r["tablename"]
            for r in await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            )
        }
        existing_views = {
            r["matviewname"]
            for r in await conn.fetch(
                "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public'"
            )
        }

        missing_t = [t for t in EXPECTED_TABLES if t not in existing_tables]
        missing_v = [v for v in EXPECTED_VIEWS  if v not in existing_views]

        if missing_t:
            print(f"  MISSING tables: {', '.join(missing_t)}")
        if missing_v:
            print(f"  MISSING views:  {', '.join(missing_v)}")
        if not missing_t and not missing_v:
            print("  All tables and views: OK")

        # Retention policies info
        ret = cfg.retention
        print(f"\nRetention policies (set in schema):")
        print(f"  gps_raw_history:  {ret.gps_raw_days} days")
        print(f"  history (raw):    {ret.history_raw_days} days")
        print(f"  history_1min:     {ret.history_1min_days} days")
        print(f"  history_1hour:    {ret.history_1hour_years} years")
        print("\nDone! CG DB-Writer v2.0.0 schema is ready.")

    finally:
        await conn.close()


async def _ensure_role(conn, rolname: str, password: str) -> None:
    import asyncpg
    exists = await conn.fetchval(
        "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1", rolname
    )
    if not exists:
        try:
            await conn.execute(
                f"CREATE ROLE {rolname} WITH LOGIN PASSWORD '{password}'"
            )
            print(f"  Created role: {rolname}")
        except asyncpg.InsufficientPrivilegeError:
            print(
                f"  WARNING: no privilege to CREATE ROLE {rolname}. "
                f"Create manually: CREATE ROLE {rolname} WITH LOGIN PASSWORD '...';"
            )
    else:
        print(f"  Role exists: {rolname}")


def main() -> None:
    p = argparse.ArgumentParser(description="CG DB-Writer v2.0.0 — setup database")
    p.add_argument("-c", "--config", default="config.yml", help="Path to config.yml")
    p.add_argument("--drop", action="store_true", help="Drop and recreate all tables")
    args = p.parse_args()

    cfg = load_config(args.config)
    asyncio.run(setup(cfg, args.drop))


if __name__ == "__main__":
    main()
