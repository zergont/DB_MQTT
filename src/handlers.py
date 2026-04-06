"""CG DB-Writer v2.1.0 — обработка входящих MQTT сообщений."""

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
from src.gps_filter import GpsFilter, GpsPoint, GpsVerdict, haversine_m
from src.history_policy import WriteDecision, _RegParams, resolve_params, should_write

logger = logging.getLogger("cg.handler")

# Кэш GPS-фильтров: router_sn → GpsFilter
_gps_filters: dict[str, GpsFilter] = {}

# Кэш последней записи: (router_sn, equip_type, panel_id, addr) → datetime
# Используется для аналоговых регистров (history) и состояний (state_events).
_last_write_ts: dict[tuple[str, str, int, int], datetime] = {}

# Предупреждение при росте кэша (не критично, но стоит мониторить)
_WRITE_TS_CACHE_WARN = 100_000

# ── Gap detection ───────────────────────────────────────────────────────────
# Трекер на уровне оборудования (router_sn, equip_type, panel_id).
# Хранит время последнего полученного пакета (ДО сжатия/фильтрации).
# Скользящее среднее интервала обновляется экспоненциально (EMA).

_GAP_MULTIPLIER = 5  # elapsed > avg_interval × N → gap

_last_packet_ts: dict[tuple[str, str, int], datetime] = {}
_avg_interval: dict[tuple[str, str, int], float] = {}  # секунды (EMA)
_EMA_ALPHA = 0.1  # сглаживание: 0.1 = медленная адаптация

# Regex для разбора топиков
_RE_TELEMETRY = re.compile(r"^cg/v1/telemetry/SN/([^/]+)$")
_RE_DECODED   = re.compile(r"^cg/v1/decoded/SN/([^/]+)/pcc/(\d+)$")


def get_gps_filter(router_sn: str, cfg: AppConfig) -> GpsFilter:
    if router_sn not in _gps_filters:
        _gps_filters[router_sn] = GpsFilter(cfg.gps_filter)
    return _gps_filters[router_sn]


# Трекер открытых gap'ов: (router_sn, equip_type, panel_id) → True если gap открыт
_open_gaps: dict[tuple[str, str, int], bool] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Startup restore
# ─────────────────────────────────────────────────────────────────────────────

async def restore_write_timestamps() -> None:
    """При старте загрузить _last_write_ts из БД для аналоговых и state-регистров."""
    async with db.pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT router_sn, equip_type, panel_id, addr, max(received_at) AS last_ts "
            "FROM history GROUP BY router_sn, equip_type, panel_id, addr"
        )
        for r in rows:
            key = (r["router_sn"], r["equip_type"], r["panel_id"], r["addr"])
            _last_write_ts[key] = r["last_ts"]
        analog_count = len(_last_write_ts)

        rows = await conn.fetch(
            "SELECT router_sn, equip_type, panel_id, addr, max(received_at) AS last_ts "
            "FROM state_events GROUP BY router_sn, equip_type, panel_id, addr"
        )
        for r in rows:
            key = (r["router_sn"], r["equip_type"], r["panel_id"], r["addr"])
            _last_write_ts[key] = r["last_ts"]

    logger.info(
        "Restored write timestamps: %d analog, %d state registers",
        analog_count,
        len(_last_write_ts) - analog_count,
    )


async def restore_gap_tracker() -> None:
    """При старте загрузить состояние gap-трекера из БД.

    1. last_seen_at из equipment → _last_packet_ts
    2. Открытые gap'ы (gap_end IS NULL) → _open_gaps
    """
    async with db.pool().acquire() as conn:
        # Восстанавливаем _last_packet_ts из equipment.last_seen_at
        equip_rows = await db.get_last_packet_times(conn)
        for r in equip_rows:
            ekey = (r["router_sn"], r["equip_type"], r["panel_id"])
            _last_packet_ts[ekey] = r["last_seen_at"]

        # Восстанавливаем _open_gaps
        gap_rows = await db.get_open_gaps(conn)
        for r in gap_rows:
            ekey = (r["router_sn"], r["equip_type"], r["panel_id"])
            _open_gaps[ekey] = True

    logger.info(
        "Restored gap tracker: %d equipment timestamps, %d open gaps",
        len(_last_packet_ts), len(_open_gaps),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch
# ─────────────────────────────────────────────────────────────────────────────

async def dispatch(topic: str, payload_bytes: bytes, cfg: AppConfig) -> None:
    """Главная точка входа: topic + raw payload → обработка."""
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
        router_sn = m_dec.group(1)
        panel_id  = int(m_dec.group(2))
        try:
            data = json.loads(payload_bytes)
        except json.JSONDecodeError as e:
            logger.warning("Bad JSON on %s: %s", topic, e)
            return
        await _handle_decoded(router_sn, panel_id, data, cfg)
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
    now: datetime,
) -> None:
    """Проверка gap'а на уровне оборудования (ДО сжатия/фильтрации).

    Логика:
    - Первый пакет: просто запоминаем ts, gap не детектируем.
    - Последующие: обновляем EMA среднего интервала.
      Если elapsed > avg_interval × _GAP_MULTIPLIER → gap.
    - Если gap был открыт → закрываем (gap_end = now).
    - Если gap обнаружен → открываем (gap_start = last_packet_ts).
    """
    ekey = (router_sn, equip_type, panel_id)
    prev_ts = _last_packet_ts.get(ekey)

    if prev_ts is not None:
        elapsed = (now - prev_ts).total_seconds()
        has_open_gap = _open_gaps.get(ekey, False)

        # Вычисляем текущий avg (без обновления — обновим после решения о gap)
        avg = _avg_interval.get(ekey)

        if avg is None:
            # Второй пакет — инициализируем EMA
            _avg_interval[ekey] = elapsed
            avg = elapsed
            is_gap = False  # Ещё нет статистики для детекции
        else:
            # Gap detection: elapsed > avg × N
            # Минимальный порог: 60 сек (чтобы не ловить мелкие задержки)
            threshold = max(avg * _GAP_MULTIPLIER, 60.0)
            is_gap = elapsed > threshold

            # Обновляем EMA только нормальными интервалами (не gap'ами)
            if not is_gap:
                _avg_interval[ekey] = _EMA_ALPHA * elapsed + (1 - _EMA_ALPHA) * avg

        if is_gap and not has_open_gap:
            # Открываем gap: gap_start = время последнего пакета ДО разрыва
            gap_id = await db.insert_data_gap(conn, router_sn, equip_type, panel_id, prev_ts)
            logger.info(
                "GAP opened id=%d %s/%s/%d: elapsed=%.0fs threshold=%.0fs avg=%.0fs",
                gap_id, router_sn, equip_type, panel_id, elapsed, threshold, avg,
            )
            # Gap сразу закрывается текущим пакетом (он же — первый после разрыва)
            count = await db.close_data_gap(conn, router_sn, equip_type, panel_id, now)
            _open_gaps[ekey] = False
            logger.info(
                "GAP closed %s/%s/%d: %d gap(s) closed",
                router_sn, equip_type, panel_id, count,
            )

        elif has_open_gap:
            # Был открытый gap (восстановлен из БД при старте), данные пришли → закрываем
            count = await db.close_data_gap(conn, router_sn, equip_type, panel_id, now)
            _open_gaps[ekey] = False
            logger.info(
                "GAP closed (restored) %s/%s/%d: %d gap(s) closed, elapsed=%.0fs",
                router_sn, equip_type, panel_id, count, elapsed,
            )

    # Обновляем время последнего пакета
    _last_packet_ts[ekey] = now


async def _handle_decoded(
    router_sn: str,
    panel_id: int,
    data: dict[str, Any],
    cfg: AppConfig,
) -> None:
    equip_type = "pcc"

    ts_str = data.get("timestamp")
    now    = datetime.now(timezone.utc)
    ts: datetime = now   # fallback: если устройство не присылает ts — используем received_at

    if ts_str:
        try:
            ts = datetime.fromisoformat(str(ts_str))
        except (ValueError, TypeError):
            logger.warning("Bad timestamp in decoded %s/%d: %s", router_sn, panel_id, ts_str)

    registers = data.get("registers")
    if not isinstance(registers, list):
        logger.warning("No registers[] in decoded %s/%d", router_sn, panel_id)
        return

    # Адреса для bulk SELECT (один раз на сообщение)
    addrs: list[int] = sorted({
        int(reg["addr"])
        for reg in registers
        if _safe_int(reg.get("addr")) is not None
    })

    # kpi_map вычисляем один раз на сообщение (fix: было внутри _process_register)
    kpi_map = cfg.history_policy.kpi_map()

    # Батчи для записи
    latest_rows_map: dict[int, tuple] = {}   # addr → tuple (последнее значение)
    history_batch:   list[tuple] = []        # аналоговые регистры
    state_batch:     list[tuple] = []        # дискретные / enum
    parameter_batch: list[tuple] = []        # уставки
    event_rows:      list[tuple] = []        # события

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await db.upsert_object(conn, router_sn)
            await db.upsert_equipment(conn, router_sn, equip_type, panel_id)

            # Gap detection — ДО обработки регистров (на уровне оборудования)
            await _check_equipment_gap(conn, router_sn, equip_type, panel_id, now)

            prev_map    = await db.get_latest_state_rows_many(conn, router_sn, equip_type, panel_id, addrs)
            catalog_map = await db.get_register_catalog_rows_many(conn, equip_type, addrs)

            for reg in registers:
                _process_register(
                    cfg=cfg, kpi_map=kpi_map,
                    router_sn=router_sn, equip_type=equip_type, panel_id=panel_id,
                    reg=reg, ts=ts, now=now,
                    prev_map=prev_map, catalog_map=catalog_map,
                    latest_rows_map=latest_rows_map,
                    history_batch=history_batch,
                    state_batch=state_batch,
                    parameter_batch=parameter_batch,
                    event_rows=event_rows,
                )

            if latest_rows_map:
                await db.upsert_latest_state_batch(conn, list(latest_rows_map.values()))
            if history_batch:
                await db.insert_history_batch(conn, history_batch)
            if state_batch:
                await db.insert_state_event_batch(conn, state_batch)
            if parameter_batch:
                await db.insert_parameter_history_batch(conn, parameter_batch)
            if event_rows:
                await db.insert_event_batch(conn, event_rows)

    logger.debug(
        "Decoded %s/pcc/%d: %d regs, latest=%d, history=%d, state=%d, param=%d, events=%d",
        router_sn, panel_id, len(registers),
        len(latest_rows_map), len(history_batch),
        len(state_batch), len(parameter_batch), len(event_rows),
    )


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
    catalog_map: dict[int, asyncpg.Record],
    latest_rows_map: dict[int, tuple],
    history_batch: list[tuple],
    state_batch: list[tuple],
    parameter_batch: list[tuple],
    event_rows: list[tuple],
) -> None:
    try:
        addr = int(reg["addr"])
    except (KeyError, TypeError, ValueError):
        logger.warning("Register without addr in %s/%d", router_sn, panel_id)
        return

    value  = reg.get("value")
    raw_val = _safe_int(reg.get("raw"))
    text   = reg.get("text")
    unit   = reg.get("unit")
    name   = reg.get("name")
    reason = reg.get("reason")

    # Преобразуем value в Decimal для numeric column
    dec_value: Decimal | None = None
    if value is not None:
        try:
            dec_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            if text is None:
                text = str(value)

    prev = prev_map.get(addr)

    # Всегда обновляем latest_state (last wins для дублирующихся addr)
    latest_rows_map[addr] = (
        router_sn, equip_type, panel_id, addr,
        ts, dec_value, raw_val, text, unit, name, reason,
    )

    # Событие: неизвестный регистр
    if reason and "Неизвестный регистр" in reason:
        if cfg.events_policy.enable_unknown_register_events:
            payload_json = json.dumps({"addr": addr, "reason": reason}, ensure_ascii=False)
            event_rows.append((
                router_sn, equip_type, panel_id,
                "unknown_register",
                f"addr={addr} reason={reason}",
                payload_json,
            ))

    # Параметры записи из catalog / kpi / defaults
    catalog_row = catalog_map.get(addr)
    params = resolve_params(cfg, equip_type, addr, catalog_row, kpi_map)
    key = (router_sn, equip_type, panel_id, addr)
    last_ts = _last_write_ts.get(key)

    prev_value  = prev["value"]  if prev else None
    prev_raw    = prev["raw"]    if prev else None
    prev_text   = prev["text"]   if prev else None
    prev_reason = prev["reason"] if prev else None

    # ── Роутинг по register_kind ──────────────────────────────────────────

    if params.register_kind == "parameter":
        # Параметры: только изменение, без heartbeat
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val, new_text=text, new_reason=reason,
            prev_value=prev_value, prev_raw=prev_raw, prev_text=prev_text, prev_reason=prev_reason,
            last_write_ts=last_ts, now=now,
            use_heartbeat=False,
        )
        if decision.write:
            parameter_batch.append((
                router_sn, equip_type, panel_id, addr, ts,
                dec_value, raw_val, text,
            ))
            _update_last_write_ts(key, now)

    elif params.register_kind in ("discrete", "enum"):
        # Состояния: change + heartbeat (для детекции gap'ов)
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val, new_text=text, new_reason=reason,
            prev_value=prev_value, prev_raw=prev_raw, prev_text=prev_text, prev_reason=prev_reason,
            last_write_ts=last_ts, now=now,
            use_heartbeat=True,
        )
        if decision.write:
            state_batch.append((
                router_sn, equip_type, panel_id, addr, ts,
                raw_val, text, decision.write_reason,
            ))
            _update_last_write_ts(key, now)

    else:
        # Аналог (default): change + heartbeat → TimescaleDB hypertable
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val, new_text=text, new_reason=reason,
            prev_value=prev_value, prev_raw=prev_raw, prev_text=prev_text, prev_reason=prev_reason,
            last_write_ts=last_ts, now=now,
            use_heartbeat=True,
        )
        if decision.write:
            history_batch.append((
                router_sn, equip_type, panel_id, addr, ts,
                dec_value, raw_val, text, reason, decision.write_reason,
            ))
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
