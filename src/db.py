"""CG DB-Writer v2.0.0 — слой работы с PostgreSQL/TimescaleDB (asyncpg)."""

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

    Tuple: (router_sn, equip_type, panel_id, addr, ts, value, raw, text, unit, name, reason)
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO latest_state
          (router_sn, equip_type, panel_id, addr, ts, value, raw, text, unit, name, reason, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
        ON CONFLICT (router_sn, equip_type, panel_id, addr) DO UPDATE SET
          ts         = EXCLUDED.ts,
          value      = EXCLUDED.value,
          raw        = EXCLUDED.raw,
          text       = EXCLUDED.text,
          unit       = EXCLUDED.unit,
          name       = EXCLUDED.name,
          reason     = EXCLUDED.reason,
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
    """Batch insert в history (аналоговые регистры).

    Tuple: (router_sn, equip_type, panel_id, addr, ts, value, raw, text, reason, write_reason)
    ts — NOT NULL (TimescaleDB hypertable partition key).
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO history
          (router_sn, equip_type, panel_id, addr, ts, value, raw, text, reason, write_reason)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """,
        rows,
    )


# ─────────────────────────────────────────────────────────────────────────────
# State events (дискретные / enum регистры → plain PostgreSQL)
# ─────────────────────────────────────────────────────────────────────────────

async def insert_state_event_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Batch insert в state_events.

    Tuple: (router_sn, equip_type, panel_id, addr, ts, raw, text, write_reason)
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO state_events
          (router_sn, equip_type, panel_id, addr, ts, raw, text, write_reason)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """,
        rows,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Parameter history (уставки / настройки → plain PostgreSQL)
# ─────────────────────────────────────────────────────────────────────────────

async def insert_parameter_history_batch(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Batch insert в parameter_history.

    Tuple: (router_sn, equip_type, panel_id, addr, ts, value, raw, text)
    """
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO parameter_history
          (router_sn, equip_type, panel_id, addr, ts, value, raw, text)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
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
# Register catalog
# ─────────────────────────────────────────────────────────────────────────────

async def get_register_catalog_rows_many(
    conn: asyncpg.Connection,
    equip_type: str,
    addrs: list[int],
) -> dict[int, asyncpg.Record]:
    """Одним запросом выбрать register_catalog для набора addr."""
    if not addrs:
        return {}
    rows = await conn.fetch(
        """
        SELECT * FROM register_catalog
        WHERE equip_type = $1 AND addr = ANY($2::int[])
        """,
        equip_type, addrs,
    )
    return {int(r["addr"]): r for r in rows}
