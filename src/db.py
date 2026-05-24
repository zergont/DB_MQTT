"""CG DB-Writer v2.1.0 — слой работы с PostgreSQL/TimescaleDB (asyncpg)."""

from __future__ import annotations

import json as _json
import logging
from datetime import datetime
from typing import Any

import asyncpg

from src.config import PostgresCfg

logger = logging.getLogger("cg.db")

_pool: asyncpg.Pool | None = None


async def init_pool(cfg: PostgresCfg) -> asyncpg.Pool:
    global _pool
    _pool = await asyncpg.create_pool(
        host=cfg.host,
        port=cfg.port,
        database=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        min_size=cfg.pool_min,
        max_size=cfg.pool_max,
    )
    logger.info("PG pool created min=%d max=%d", cfg.pool_min, cfg.pool_max)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PG pool closed")


def pool() -> asyncpg.Pool:
    assert _pool is not None, "PG pool not initialised"
    return _pool


# ─────────────────────────────────────────────────────────────────────────────
# Objects / Equipment
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_object(conn: asyncpg.Connection, router_sn: str) -> None:
    await conn.execute(
        """
        INSERT INTO objects (router_sn) VALUES ($1)
        ON CONFLICT (router_sn) DO UPDATE SET updated_at = now()
        """,
        router_sn,
    )


async def upsert_equipment(
    conn: asyncpg.Connection,
    router_sn: str,
    equip_type: str,
    panel_id: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO equipment (router_sn, equip_type, panel_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (router_sn, equip_type, panel_id)
        DO UPDATE SET last_seen_at = now()
        """,
        router_sn, equip_type, panel_id,
    )


async def get_all_equipment(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Все записи equipment с метаданными объекта."""
    return await conn.fetch(
        """
        SELECT
            e.router_sn, e.equip_type, e.panel_id,
            e.name, e.manufacturer, e.model, e.engine_sn,
            e.first_seen_at, e.last_seen_at,
            o.name AS object_name
        FROM equipment e
        JOIN objects o ON o.router_sn = e.router_sn
        ORDER BY e.router_sn, e.equip_type, e.panel_id
        """
    )


async def update_equipment_meta(
    conn: asyncpg.Connection,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    name: str | None,
    manufacturer: str | None,
    model: str | None,
    engine_sn: str | None,
) -> None:
    """Обновить метаданные оборудования (название, производитель, модель, серийник)."""
    await conn.execute(
        """
        UPDATE equipment
        SET name=$4, manufacturer=$5, model=$6, engine_sn=$7
        WHERE router_sn=$1 AND equip_type=$2 AND panel_id=$3
        """,
        router_sn, equip_type, panel_id, name, manufacturer, model, engine_sn,
    )


# ─────────────────────────────────────────────────────────────────────────────
# GPS
# ─────────────────────────────────────────────────────────────────────────────

async def insert_gps_raw(
    conn: asyncpg.Connection,
    router_sn: str,
    gps_time: datetime | None,
    lat: float,
    lon: float,
    satellites: int | None,
    fix_status: int | None,
    accepted: bool,
    reject_reason: str | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO gps_raw_history
          (router_sn, gps_time, lat, lon, satellites, fix_status, accepted, reject_reason)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        router_sn, gps_time, lat, lon, satellites, fix_status, accepted, reject_reason,
    )


async def upsert_gps_latest(
    conn: asyncpg.Connection,
    router_sn: str,
    gps_time: datetime | None,
    lat: float,
    lon: float,
    satellites: int | None,
    fix_status: int | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO gps_latest_filtered
          (router_sn, gps_time, received_at, lat, lon, satellites, fix_status)
        VALUES ($1, $2, now(), $3, $4, $5, $6)
        ON CONFLICT (router_sn) DO UPDATE SET
          gps_time    = EXCLUDED.gps_time,
          received_at = EXCLUDED.received_at,
          lat         = EXCLUDED.lat,
          lon         = EXCLUDED.lon,
          satellites  = EXCLUDED.satellites,
          fix_status  = EXCLUDED.fix_status
        """,
        router_sn, gps_time, lat, lon, satellites, fix_status,
    )


async def get_gps_latest(
    conn: asyncpg.Connection,
    router_sn: str,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM gps_latest_filtered WHERE router_sn = $1",
        router_sn,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Latest state
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_latest_state_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Batch upsert latest_state.

    Tuple: (router_sn, equip_type, panel_id, addr, ts, value, raw)
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO latest_state
          (router_sn, equip_type, panel_id, addr, ts, value, raw, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7, now())
        ON CONFLICT (router_sn, equip_type, panel_id, addr) DO UPDATE SET
          ts         = EXCLUDED.ts,
          value      = EXCLUDED.value,
          raw        = EXCLUDED.raw,
          updated_at = now()
        """,
        rows,
    )


async def get_latest_state_rows_many(
    conn: asyncpg.Connection,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    addrs: list[int],
) -> dict[int, asyncpg.Record]:
    """Одним запросом выбрать latest_state для набора addr."""
    if not addrs:
        return {}
    rows = await conn.fetch(
        """
        SELECT * FROM latest_state
        WHERE router_sn=$1 AND equip_type=$2 AND panel_id=$3 AND addr = ANY($4::int[])
        """,
        router_sn, equip_type, panel_id, addrs,
    )
    return {int(r["addr"]): r for r in rows}


# ─────────────────────────────────────────────────────────────────────────────
# History (аналоговые регистры → TimescaleDB hypertable)
# ─────────────────────────────────────────────────────────────────────────────

async def insert_history_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Batch insert в history (все типы регистров: аналог, enum, fault raw).

    Tuple: (router_sn, equip_type, panel_id, addr, ts, value, raw)
    ts — NOT NULL (TimescaleDB hypertable partition key).
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO history
          (router_sn, equip_type, panel_id, addr, ts, value, raw)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
        rows,
    )



# ─────────────────────────────────────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────────────────────────────────────

async def insert_event(
    conn: asyncpg.Connection,
    router_sn: str,
    event_type: str,
    description: str | None = None,
    equip_type: str | None = None,
    panel_id: int | None = None,
    payload: Any | None = None,
) -> None:
    payload_json = _json.dumps(payload, ensure_ascii=False) if payload else None
    await conn.execute(
        """
        INSERT INTO events (router_sn, equip_type, panel_id, type, description, payload)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb)
        """,
        router_sn, equip_type, panel_id, event_type, description, payload_json,
    )


async def insert_event_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Batch insert events.

    Tuple: (router_sn, equip_type, panel_id, type, description, payload_json_str_or_none)
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO events (router_sn, equip_type, panel_id, type, description, payload)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb)
        """,
        rows,
    )



# ─────────────────────────────────────────────────────────────────────────────
# Data gaps — разрывы связи с оборудованием
# ─────────────────────────────────────────────────────────────────────────────

async def insert_data_gap(
    conn: asyncpg.Connection,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    gap_start: datetime,
) -> int:
    """Открыть новый gap (gap_end = NULL → ongoing). Возвращает id."""
    row = await conn.fetchrow(
        """
        INSERT INTO data_gaps (router_sn, equip_type, panel_id, gap_start)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        router_sn, equip_type, panel_id, gap_start,
    )
    return row["id"]


async def close_data_gap(
    conn: asyncpg.Connection,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    gap_end: datetime,
) -> int:
    """Закрыть открытый gap (gap_end IS NULL). Возвращает кол-во обновлённых."""
    result = await conn.execute(
        """
        UPDATE data_gaps
        SET gap_end = $4
        WHERE router_sn = $1 AND equip_type = $2 AND panel_id = $3
          AND gap_end IS NULL
        """,
        router_sn, equip_type, panel_id, gap_end,
    )
    # asyncpg returns "UPDATE N"
    return int(result.split()[-1])


async def get_open_gaps(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Все незакрытые gap'ы (для восстановления при старте)."""
    return await conn.fetch(
        """
        SELECT router_sn, equip_type, panel_id, gap_start
        FROM data_gaps
        WHERE gap_end IS NULL
        """
    )


async def get_last_packet_times(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Последнее время пакета для каждого оборудования (equipment.last_seen_at)."""
    return await conn.fetch(
        "SELECT router_sn, equip_type, panel_id, last_seen_at FROM equipment"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fault history — история активности fault-битов
# ─────────────────────────────────────────────────────────────────────────────

async def open_fault_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Открыть новые fault-записи (fault_end = NULL).

    Tuple: (router_sn, equip_type, panel_id, addr, bit, fault_start)
    name/severity живут в cg/v1/maps/<device_type>, не в БД.
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO fault_history
          (router_sn, equip_type, panel_id, addr, bit, fault_start)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        rows,
    )


async def close_faults_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Закрыть активные fault-записи (fault_end IS NULL → fault_end = ts).

    Tuple: (router_sn, equip_type, panel_id, addr, bit, fault_end)
    """
    if not rows:
        return
    await conn.executemany(
        """
        UPDATE fault_history
        SET fault_end = $6
        WHERE router_sn = $1 AND equip_type = $2 AND panel_id = $3
          AND addr = $4 AND bit = $5
          AND fault_end IS NULL
        """,
        rows,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Enum history — периоды активности enum-состояний
# ─────────────────────────────────────────────────────────────────────────────

async def open_enum_state_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Открыть новые enum-состояния (state_end = NULL).

    Tuple: (router_sn, equip_type, panel_id, addr, value, state_start)
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO enum_history
          (router_sn, equip_type, panel_id, addr, value, state_start)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        rows,
    )


async def close_enum_states_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Закрыть активные enum-состояния (state_end IS NULL → state_end = ts).

    Tuple: (router_sn, equip_type, panel_id, addr, state_end)
    """
    if not rows:
        return
    await conn.executemany(
        """
        UPDATE enum_history
        SET state_end = $5
        WHERE router_sn=$1 AND equip_type=$2 AND panel_id=$3 AND addr=$4
          AND state_end IS NULL
        """,
        rows,
    )


async def get_open_enum_states(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Все незакрытые enum-состояния (для восстановления при старте)."""
    return await conn.fetch(
        """
        SELECT router_sn, equip_type, panel_id, addr, value
        FROM enum_history
        WHERE state_end IS NULL
        """
    )


async def get_open_fault_bits(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Все активные fault-биты (для восстановления при старте)."""
    return await conn.fetch(
        """
        SELECT router_sn, equip_type, panel_id, addr, bit
        FROM fault_history
        WHERE fault_end IS NULL
        """
    )
