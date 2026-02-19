"""
CG DB-Writer — обработка входящих MQTT сообщений.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

from src import db
from src.config import AppConfig
from src.gps_filter import GpsFilter, GpsPoint, GpsVerdict, _haversine_m
from src.history_policy import HistoryDecision, resolve_params, should_write_history

logger = logging.getLogger("cg.handler")

# Кэш GPS-фильтров: router_sn → GpsFilter
_gps_filters: dict[str, GpsFilter] = {}

# Кэш последнего ts записи в history: (router_sn, equip_type, panel_id, addr) → datetime
_last_history_ts: dict[tuple[str, str, int, int], datetime] = {}

# Regex для разбора топиков
_RE_TELEMETRY = re.compile(r"^cg/v1/telemetry/SN/([^/]+)$")
_RE_DECODED = re.compile(r"^cg/v1/decoded/SN/([^/]+)/pcc/(\d+)$")


def get_gps_filter(router_sn: str, cfg: AppConfig) -> GpsFilter:
    if router_sn not in _gps_filters:
        _gps_filters[router_sn] = GpsFilter(cfg.gps_filter)
    return _gps_filters[router_sn]


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

async def dispatch(
    topic: str,
    payload_bytes: bytes,
    cfg: AppConfig,
    last_seen: dict[str, datetime],
    panel_last_seen: dict[tuple[str, int], datetime],
) -> None:
    """Главная точка входа: topic + raw payload → обработка."""
    m_tel = _RE_TELEMETRY.match(topic)
    if m_tel:
        router_sn = m_tel.group(1)
        last_seen[router_sn] = datetime.now(timezone.utc)
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError as e:
            logger.warning("Bad JSON on %s: %s", topic, e)
            return
        await _handle_telemetry(router_sn, data, cfg)
        return

    m_dec = _RE_DECODED.match(topic)
    if m_dec:
        router_sn = m_dec.group(1)
        panel_id = int(m_dec.group(2))
        last_seen[router_sn] = datetime.now(timezone.utc)
        panel_last_seen[(router_sn, panel_id)] = datetime.now(timezone.utc)
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError as e:
            logger.warning("Bad JSON on %s: %s", topic, e)
            return
        await _handle_decoded(router_sn, panel_id, data, cfg)
        return

    logger.debug("Unknown topic: %s", topic)


# ---------------------------------------------------------------------------
# Telemetry (GPS)
# ---------------------------------------------------------------------------

def _parse_gps_time(gps: dict[str, Any]) -> datetime | None:
    """Извлечь время из GPS блока."""
    iso = gps.get("date_iso_8601")
    if iso:
        try:
            return datetime.fromisoformat(iso)
        except (ValueError, TypeError):
            pass
    ts = gps.get("timestamp")
    if ts:
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            pass
    return None


async def _handle_telemetry(
    router_sn: str,
    data: dict[str, Any],
    cfg: AppConfig,
) -> None:
    gps = data.get("GPS")
    if not gps:
        logger.debug("Telemetry %s: no GPS key", router_sn)
        return

    try:
        lat = float(gps["latitude"])
        lon = float(gps["longitude"])
    except (KeyError, TypeError, ValueError) as e:
        logger.warning("GPS parse error for %s: %s", router_sn, e)
        return

    satellites = _safe_int(gps.get("satellites"))
    fix_status = _safe_int(gps.get("fix_status"))
    gps_time = _parse_gps_time(gps)
    now = datetime.now(timezone.utc)

    pt = GpsPoint(
        lat=lat, lon=lon,
        satellites=satellites, fix_status=fix_status,
        gps_time=gps_time, received_at=now,
    )

    flt = get_gps_filter(router_sn, cfg)
    verdict: GpsVerdict = flt.check(pt)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await db.upsert_object(conn, router_sn)

            # Всегда пишем raw
            await db.insert_gps_raw(
                conn, router_sn, gps_time, lat, lon,
                satellites, fix_status,
                verdict.accepted, verdict.reject_reason,
            )

            if verdict.accepted:
                # Обновляем latest только если за пределами deadband
                # (если accepted и внутри deadband — всё равно accepted,
                #  но latest можно не трогать)
                last = flt.last_accepted
                update_latest = True
                if last is not None:
                    prev = await db.get_gps_latest(conn, router_sn)
                    if prev is not None:
                        d = _haversine_m(prev["lat"], prev["lon"], lat, lon)
                        if d < cfg.gps_filter.deadband_m:
                            update_latest = False

                if update_latest:
                    await db.upsert_gps_latest(
                        conn, router_sn, gps_time, lat, lon,
                        satellites, fix_status,
                    )

            elif cfg.events_policy.enable_gps_reject_events:
                await db.insert_event(
                    conn, router_sn,
                    "gps_jump_rejected",
                    description=f"reason={verdict.reject_reason} lat={lat} lon={lon}",
                    payload={
                        "lat": lat, "lon": lon,
                        "reject_reason": verdict.reject_reason,
                        "satellites": satellites,
                    },
                )

    logger.debug(
        "GPS %s: accepted=%s reason=%s lat=%.6f lon=%.6f",
        router_sn, verdict.accepted, verdict.reject_reason, lat, lon,
    )


# ---------------------------------------------------------------------------
# Decoded (Панели)
# ---------------------------------------------------------------------------

async def _handle_decoded(
    router_sn: str,
    panel_id: int,
    data: dict[str, Any],
    cfg: AppConfig,
) -> None:
    equip_type = "pcc"

    ts_str = data.get("timestamp")
    ts: datetime | None = None
    if ts_str:
        try:
            ts = datetime.fromisoformat(str(ts_str))
        except (ValueError, TypeError):
            logger.warning("Bad timestamp in decoded %s/%d: %s", router_sn, panel_id, ts_str)

    registers = data.get("registers")
    if not isinstance(registers, list):
        logger.warning("No registers[] in decoded %s/%d", router_sn, panel_id)
        return

    now = datetime.now(timezone.utc)
    history_batch: list[tuple] = []

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await db.upsert_object(conn, router_sn)
            await db.upsert_equipment(conn, router_sn, equip_type, panel_id)

            for reg in registers:
                await _process_register(
                    conn, cfg, router_sn, equip_type, panel_id,
                    reg, ts, now, history_batch,
                )

            if history_batch:
                await db.insert_history_batch(conn, history_batch)

    logger.debug(
        "Decoded %s/pcc/%d: %d regs, %d history writes",
        router_sn, panel_id, len(registers), len(history_batch),
    )


async def _process_register(
    conn: asyncpg.Connection,
    cfg: AppConfig,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    reg: dict[str, Any],
    ts: datetime | None,
    now: datetime,
    history_batch: list[tuple],
) -> None:
    try:
        addr = int(reg["addr"])
    except (KeyError, TypeError, ValueError):
        logger.warning("Register without addr in %s/%d", router_sn, panel_id)
        return

    value = reg.get("value")
    raw_val = _safe_int(reg.get("raw"))
    text = reg.get("text")
    unit = reg.get("unit")
    name = reg.get("name")
    reason = reg.get("reason")

    # Преобразуем value в Decimal для numeric column
    dec_value: Decimal | None = None
    if value is not None:
        try:
            dec_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            # Не числовое значение — сохраняем как text, value=null
            if text is None:
                text = str(value)
            dec_value = None

    # --- Получаем предыдущее состояние ---
    prev = await db.get_latest_state_row(conn, router_sn, equip_type, panel_id, addr)

    # --- Upsert latest_state ---
    await db.upsert_latest_state(
        conn, router_sn, equip_type, panel_id, addr,
        ts, dec_value, raw_val, text, unit, name, reason,
    )

    # --- Event: unknown register ---
    if reason and "Неизвестный регистр" in reason:
        if cfg.events_policy.enable_unknown_register_events:
            await db.insert_event(
                conn, router_sn,
                "unknown_register",
                description=f"addr={addr} reason={reason}",
                equip_type=equip_type,
                panel_id=panel_id,
                payload={"addr": addr, "reason": reason},
            )

    # --- History decision ---
    catalog_row = await db.get_register_catalog_row(conn, equip_type, addr)
    params = resolve_params(cfg, addr, catalog_row)

    key = (router_sn, equip_type, panel_id, addr)
    last_h_ts = _last_history_ts.get(key)

    decision: HistoryDecision = should_write_history(
        params,
        new_value=dec_value,
        new_raw=raw_val,
        new_text=text,
        new_reason=reason,
        prev_value=prev["value"] if prev else None,
        prev_raw=prev["raw"] if prev else None,
        prev_text=prev["text"] if prev else None,
        prev_reason=prev["reason"] if prev else None,
        last_history_ts=last_h_ts,
        now=now,
    )

    if decision.write:
        history_batch.append((
            router_sn, equip_type, panel_id, addr,
            ts, dec_value, raw_val, text, reason, decision.write_reason,
        ))
        _last_history_ts[key] = now


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
