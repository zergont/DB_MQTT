# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""CG DB-Writer — обработка входящих MQTT сообщений."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

from src import db, register_map
from src.config import AppConfig, GapDetectorCfg
from src.gps_filter import GpsFilter, GpsPoint, GpsVerdict, haversine_m
from src.history_policy import WriteDecision, resolve_params, should_write

logger = logging.getLogger("cg.handler")

# Кэш GPS-фильтров: router_sn → GpsFilter
_gps_filters: dict[str, GpsFilter] = {}

# Кэш последней записи: (router_sn, equip_type, panel_id, addr) → datetime
_last_write_ts: dict[tuple[str, str, int, int], datetime] = {}
_WRITE_TS_CACHE_WARN = 100_000

# ── Gap detection ───────────────────────────────────────────────────────────
# Трекер на уровне оборудования (router_sn, equip_type, panel_id).
# Использует device-time (ts из payload) для корректной обработки буферизации.

_last_packet_ts: dict[tuple[str, str, int], datetime] = {}
_avg_interval:   dict[tuple[str, str, int], float] = {}

# Трекер открытых gap'ов
_open_gaps: dict[tuple[str, str, int], bool] = {}

# Кэш активных fault-битов: (router_sn, equip_type, panel_id, addr) → set of bit numbers
_active_fault_bits: dict[tuple[str, str, int, int], set[int]] = {}

# Кэш активных enum-состояний: (router_sn, equip_type, panel_id, addr) → value (int)
_active_enum_states: dict[tuple[str, str, int, int], int] = {}

# Regex для разбора топиков
_RE_TELEMETRY = re.compile(r"^cg/v1/telemetry/SN/([^/]+)$")
_RE_DECODED   = re.compile(r"^cg/v1/decoded/SN/([^/]+)/([^/]+)/(\d+)$")
_RE_MAPS      = re.compile(r"^cg/v1/maps/([^/]+)$")


def get_gps_filter(router_sn: str, cfg: AppConfig) -> GpsFilter:
    if router_sn not in _gps_filters:
        _gps_filters[router_sn] = GpsFilter(cfg.gps_filter)
    return _gps_filters[router_sn]


# ─────────────────────────────────────────────────────────────────────────────
# Startup restore
# ─────────────────────────────────────────────────────────────────────────────

async def restore_write_timestamps() -> None:
    """При старте загрузить _last_write_ts из history."""
    async with db.pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT router_sn, equip_type, panel_id, addr, max(received_at) AS last_ts "
            "FROM history GROUP BY router_sn, equip_type, panel_id, addr"
        )
        for r in rows:
            key = (r["router_sn"], r["equip_type"], r["panel_id"], r["addr"])
            _last_write_ts[key] = r["last_ts"]

    logger.info("Restored write timestamps: %d registers", len(_last_write_ts))


async def restore_fault_bits() -> None:
    """При старте загрузить активные fault-биты из fault_history WHERE fault_end IS NULL."""
    async with db.pool().acquire() as conn:
        rows = await db.get_open_fault_bits(conn)
        for r in rows:
            key = (r["router_sn"], r["equip_type"], r["panel_id"], r["addr"])
            if key not in _active_fault_bits:
                _active_fault_bits[key] = set()
            _active_fault_bits[key].add(r["bit"])
    logger.info(
        "Restored fault bits: %d active faults across %d registers",
        sum(len(v) for v in _active_fault_bits.values()), len(_active_fault_bits),
    )


async def restore_enum_states() -> None:
    """При старте загрузить активные enum-состояния из enum_history WHERE state_end IS NULL."""
    async with db.pool().acquire() as conn:
        rows = await db.get_open_enum_states(conn)
        for r in rows:
            key = (r["router_sn"], r["equip_type"], r["panel_id"], r["addr"])
            _active_enum_states[key] = int(r["value"])
    logger.info("Restored enum states: %d open states", len(_active_enum_states))


async def restore_gap_tracker() -> None:
    """При старте восстановить открытые gap'ы из data_gaps.

    _last_packet_ts не восстанавливаем: после рестарта первый пакет
    инициализирует baseline по device-time. Открытые gap'ы закроются
    при получении следующего пакета.
    """
    async with db.pool().acquire() as conn:
        gap_rows = await db.get_open_gaps(conn)
        for r in gap_rows:
            ekey = (r["router_sn"], r["equip_type"], r["panel_id"])
            _open_gaps[ekey] = True

    logger.info("Restored gap tracker: %d open gaps", len(_open_gaps))


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch
# ─────────────────────────────────────────────────────────────────────────────

async def dispatch(topic: str, payload_bytes: bytes, cfg: AppConfig) -> None:
    """Главная точка входа: topic + raw payload → обработка."""

    # Register map (retained, обновляется при старте и при изменениях)
    m_maps = _RE_MAPS.match(topic)
    if m_maps:
        device_type = m_maps.group(1)
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError as e:
            logger.warning("Bad JSON on maps %s: %s", topic, e)
            return
        register_map.update(device_type, data)
        async with db.pool().acquire() as conn:
            await register_map.sync_to_db(conn, device_type)
        return

    m_tel = _RE_TELEMETRY.match(topic)
    if m_tel:
        router_sn = m_tel.group(1)
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError as e:
            logger.warning("Bad JSON on %s: %s", topic, e)
            return
        await _handle_telemetry(router_sn, data, cfg)
        return

    m_dec = _RE_DECODED.match(topic)
    if m_dec:
        router_sn  = m_dec.group(1)
        equip_type = m_dec.group(2)
        panel_id   = int(m_dec.group(3))
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError as e:
            logger.warning("Bad JSON on %s: %s", topic, e)
            return
        await _handle_decoded(router_sn, equip_type, panel_id, data, cfg)
        return

    logger.debug("Unknown topic: %s", topic)


# ─────────────────────────────────────────────────────────────────────────────
# Telemetry (GPS)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_gps_time(gps: dict[str, Any]) -> datetime | None:
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
    gps_time   = _parse_gps_time(gps)
    now        = datetime.now(timezone.utc)

    pt      = GpsPoint(lat=lat, lon=lon, satellites=satellites,
                       fix_status=fix_status, gps_time=gps_time, received_at=now)
    flt     = get_gps_filter(router_sn, cfg)
    verdict: GpsVerdict = flt.check(pt)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await db.upsert_object(conn, router_sn)

            await db.insert_gps_raw(
                conn, router_sn, gps_time, lat, lon,
                satellites, fix_status, verdict.accepted, verdict.reject_reason,
            )

            if verdict.accepted:
                update_latest = True
                if flt.last_accepted is not None:
                    prev = await db.get_gps_latest(conn, router_sn)
                    if prev is not None:
                        d = haversine_m(prev["lat"], prev["lon"], lat, lon)
                        if d < cfg.gps_filter.deadband_m:
                            update_latest = False

                if update_latest:
                    await db.upsert_gps_latest(
                        conn, router_sn, gps_time, lat, lon, satellites, fix_status,
                    )

            elif cfg.events_policy.enable_gps_reject_events:
                await db.insert_event(
                    conn, router_sn, "gps_jump_rejected",
                    description=f"reason={verdict.reject_reason} lat={lat} lon={lon}",
                    payload={"lat": lat, "lon": lon,
                             "reject_reason": verdict.reject_reason,
                             "satellites": satellites},
                )

    logger.debug(
        "GPS %s: accepted=%s reason=%s lat=%.6f lon=%.6f",
        router_sn, verdict.accepted, verdict.reject_reason, lat, lon,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Decoded (панели)
# ─────────────────────────────────────────────────────────────────────────────

async def _check_equipment_gap(
    conn: asyncpg.Connection,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    ts: datetime,          # device time из payload
    gap_cfg: GapDetectorCfg,
) -> None:
    """Проверка gap'а на уровне оборудования.

    Использует device-time (ts) для корректной обработки буферизации:
    если роутер потерял связь, накопил пакеты и выслал их пачкой,
    интервалы по device-time будут нормальными — ложного gap'а не будет.
    """
    ekey = (router_sn, equip_type, panel_id)
    prev_ts = _last_packet_ts.get(ekey)

    if prev_ts is not None:
        elapsed = (ts - prev_ts).total_seconds()

        # Защита от отрицательного elapsed (пакеты пришли не по порядку)
        if elapsed < 0:
            _last_packet_ts[ekey] = ts
            return

        has_open_gap = _open_gaps.get(ekey, False)
        avg = _avg_interval.get(ekey)

        if avg is None:
            _avg_interval[ekey] = elapsed
            avg = elapsed
            is_gap = False
        else:
            threshold = max(avg * gap_cfg.multiplier, float(gap_cfg.min_threshold_sec))
            is_gap = elapsed > threshold
            if not is_gap:
                _avg_interval[ekey] = gap_cfg.ema_alpha * elapsed + (1 - gap_cfg.ema_alpha) * avg

        if is_gap and not has_open_gap:
            gap_id = await db.insert_data_gap(conn, router_sn, equip_type, panel_id, prev_ts)
            logger.info(
                "GAP opened id=%d %s/%s/%d: elapsed=%.0fs threshold=%.0fs avg=%.0fs",
                gap_id, router_sn, equip_type, panel_id, elapsed, threshold, avg,
            )
            count = await db.close_data_gap(conn, router_sn, equip_type, panel_id, ts)
            _open_gaps[ekey] = False
            logger.info(
                "GAP closed %s/%s/%d: %d gap(s) closed",
                router_sn, equip_type, panel_id, count,
            )

        elif has_open_gap:
            count = await db.close_data_gap(conn, router_sn, equip_type, panel_id, ts)
            _open_gaps[ekey] = False
            logger.info(
                "GAP closed (restored) %s/%s/%d: %d gap(s) closed, elapsed=%.0fs",
                router_sn, equip_type, panel_id, count, elapsed,
            )

    _last_packet_ts[ekey] = ts


async def _handle_decoded(
    router_sn: str,
    equip_type: str,
    panel_id: int,
    data: dict[str, Any],
    cfg: AppConfig,
) -> None:

    ts_str = data.get("timestamp")
    now    = datetime.now(timezone.utc)
    ts: datetime = now

    if ts_str:
        try:
            ts = datetime.fromisoformat(str(ts_str))
        except (ValueError, TypeError):
            logger.warning("Bad timestamp in decoded %s/%d: %s", router_sn, panel_id, ts_str)

    registers = data.get("registers")
    if not isinstance(registers, list):
        logger.warning("No registers[] in decoded %s/%d", router_sn, panel_id)
        return

    addrs: list[int] = sorted({
        int(reg["addr"])
        for reg in registers
        if _safe_int(reg.get("addr")) is not None
    })

    kpi_map = cfg.history_policy.kpi_map()

    # Батчи для записи
    latest_rows_map:  dict[int, tuple] = {}
    history_batch:    list[tuple] = []
    enum_open_batch:  list[tuple] = []
    enum_close_batch: list[tuple] = []
    fault_open_batch:  list[tuple] = []
    fault_close_batch: list[tuple] = []
    event_rows:        list[tuple] = []

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await db.upsert_object(conn, router_sn)
            await db.upsert_equipment(conn, router_sn, equip_type, panel_id)

            # Gap detection по device-time
            await _check_equipment_gap(conn, router_sn, equip_type, panel_id, ts, cfg.gap_detector)

            prev_map = await db.get_latest_state_rows_many(
                conn, router_sn, equip_type, panel_id, addrs,
            )

            for reg in registers:
                value = reg.get("value")

                # Fault bitmap: value приходит как dict с расшифровкой битов
                if isinstance(value, dict):
                    addr = _safe_int(reg.get("addr"))
                    if addr is not None:
                        _process_fault_bitmap(
                            router_sn=router_sn, equip_type=equip_type, panel_id=panel_id,
                            addr=addr, raw_val=_safe_int(reg.get("raw")),
                            value_dict=value, ts=ts,
                            prev_map=prev_map,
                            latest_rows_map=latest_rows_map,
                            history_batch=history_batch,
                            fault_open_batch=fault_open_batch,
                            fault_close_batch=fault_close_batch,
                            event_rows=event_rows,
                            cfg=cfg,
                        )
                    continue

                _process_register(
                    cfg=cfg, kpi_map=kpi_map,
                    router_sn=router_sn, equip_type=equip_type, panel_id=panel_id,
                    reg=reg, ts=ts, now=now,
                    prev_map=prev_map,
                    latest_rows_map=latest_rows_map,
                    history_batch=history_batch,
                    enum_open_batch=enum_open_batch,
                    enum_close_batch=enum_close_batch,
                    event_rows=event_rows,
                )

            if latest_rows_map:
                await db.upsert_latest_state_batch(conn, list(latest_rows_map.values()))
            if history_batch:
                await db.insert_history_batch(conn, history_batch)
            if enum_close_batch:                            # сначала закрываем старые
                await db.close_enum_states_batch(conn, enum_close_batch)
            if enum_open_batch:                             # потом открываем новые
                await db.open_enum_state_batch(conn, enum_open_batch)
            if fault_close_batch:                           # сначала закрываем старые
                await db.close_faults_batch(conn, fault_close_batch)
            if fault_open_batch:                            # потом открываем новые
                await db.open_fault_batch(conn, fault_open_batch)
            if event_rows:
                await db.insert_event_batch(conn, event_rows)

    logger.debug(
        "Decoded %s/%s/%d: %d regs, latest=%d, history=%d, "
        "enum_open=%d enum_close=%d, fault_open=%d fault_close=%d, events=%d",
        router_sn, equip_type, panel_id, len(registers),
        len(latest_rows_map), len(history_batch),
        len(enum_open_batch), len(enum_close_batch),
        len(fault_open_batch), len(fault_close_batch),
        len(event_rows),
    )


def _process_fault_bitmap(
    *,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    addr: int,
    raw_val: int | None,
    value_dict: dict[str, Any],
    ts: datetime,
    prev_map: dict[int, asyncpg.Record],
    latest_rows_map: dict[int, tuple],
    history_batch: list[tuple],
    fault_open_batch: list[tuple],
    fault_close_batch: list[tuple],
    event_rows: list[tuple],
    cfg: AppConfig,
) -> None:
    """Обработка fault bitmap регистра.

    - Открываем/закрываем fault-биты в fault_history.
    - В history пишем raw (16-бит маска) при изменении.
    - name/severity живут в cg/v1/maps/<device_type>, не в БД.
    - В events пишем только {addr, bit}.
    """
    faults_list: list[dict] = value_dict.get("faults") or []
    current_bits: set[int] = {f["bit"] for f in faults_list if "bit" in f}

    fkey = (router_sn, equip_type, panel_id, addr)
    prev_bits: set[int] = _active_fault_bits.get(fkey, set())

    appeared = current_bits - prev_bits
    cleared  = prev_bits - current_bits

    for bit in appeared:
        fault_open_batch.append((router_sn, equip_type, panel_id, addr, bit, ts))
        if cfg.events_policy.enable_fault_events:
            payload_json = json.dumps({"addr": addr, "bit": bit}, ensure_ascii=False)
            event_rows.append((
                router_sn, equip_type, panel_id,
                "fault",
                f"addr={addr} bit={bit}",
                payload_json,
            ))
        logger.info(
            "FAULT appeared %s/%s/%d addr=%d bit=%d",
            router_sn, equip_type, panel_id, addr, bit,
        )

    for bit in cleared:
        fault_close_batch.append((router_sn, equip_type, panel_id, addr, bit, ts))
        logger.info(
            "FAULT cleared %s/%s/%d addr=%d bit=%d",
            router_sn, equip_type, panel_id, addr, bit,
        )

    _active_fault_bits[fkey] = current_bits

    # История raw-значения в history (только при изменении)
    prev = prev_map.get(addr)
    prev_raw = prev["raw"] if prev else None
    dec_raw = Decimal(str(raw_val)) if raw_val is not None else None

    if raw_val != prev_raw:
        history_batch.append((router_sn, equip_type, panel_id, addr, ts, dec_raw, raw_val))

    # latest_state
    latest_rows_map[addr] = (router_sn, equip_type, panel_id, addr, ts, dec_raw, raw_val)


def _process_register(
    *,
    cfg: AppConfig,
    kpi_map: dict,
    router_sn: str,
    equip_type: str,
    panel_id: int,
    reg: dict[str, Any],
    ts: datetime,
    now: datetime,
    prev_map: dict[int, asyncpg.Record],
    latest_rows_map: dict[int, tuple],
    history_batch: list[tuple],
    enum_open_batch: list[tuple],
    enum_close_batch: list[tuple],
    event_rows: list[tuple],
) -> None:
    try:
        addr = int(reg["addr"])
    except (KeyError, TypeError, ValueError):
        logger.warning("Register without addr in %s/%d", router_sn, panel_id)
        return

    # Пропускаем регистры с ошибкой декодирования
    if reg.get("reason"):
        return

    value   = reg.get("value")
    raw_val = _safe_int(reg.get("raw"))

    # Тип регистра из in-memory map
    map_unit = register_map.get_unit(equip_type, addr)

    # Конвертируем value в Decimal для numeric column
    dec_value: Decimal | None = None
    if value is not None:
        try:
            dec_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            pass

    prev       = prev_map.get(addr)
    prev_value = prev["value"] if prev else None
    prev_raw   = prev["raw"]   if prev else None

    # latest_state — обновляем всегда
    latest_rows_map[addr] = (router_sn, equip_type, panel_id, addr, ts, dec_value, raw_val)

    params = resolve_params(cfg, equip_type, addr, map_unit, kpi_map)
    key    = (router_sn, equip_type, panel_id, addr)
    last_ts = _last_write_ts.get(key)

    if params.register_kind == "enum":
        # ── Enum: только change, без heartbeat ───────────────────────────
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val,
            prev_value=prev_value, prev_raw=prev_raw,
            last_write_ts=last_ts, now=now,
            use_heartbeat=False,
        )
        if decision.write:
            history_batch.append((router_sn, equip_type, panel_id, addr, ts, dec_value, raw_val))
            _update_last_write_ts(key, now)

        # enum_history: открываем/закрываем периоды состояния
        ekey = (router_sn, equip_type, panel_id, addr)
        prev_enum_val = _active_enum_states.get(ekey)
        if raw_val != prev_enum_val:
            if prev_enum_val is not None:
                enum_close_batch.append((router_sn, equip_type, panel_id, addr, ts))
            if raw_val is not None:
                enum_open_batch.append((router_sn, equip_type, panel_id, addr, raw_val, ts))
            _active_enum_states[ekey] = raw_val

    else:
        # ── Аналог (default): change + tolerance + heartbeat ─────────────
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val,
            prev_value=prev_value, prev_raw=prev_raw,
            last_write_ts=last_ts, now=now,
            use_heartbeat=True,
        )
        if decision.write:
            history_batch.append((router_sn, equip_type, panel_id, addr, ts, dec_value, raw_val))
            _update_last_write_ts(key, now)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _update_last_write_ts(key: tuple, ts: datetime) -> None:
    _last_write_ts[key] = ts
    if len(_last_write_ts) > _WRITE_TS_CACHE_WARN:
        logger.warning(
            "_last_write_ts cache size=%d exceeds %d — possible memory growth",
            len(_last_write_ts), _WRITE_TS_CACHE_WARN,
        )
