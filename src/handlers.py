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

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
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

# Последняя запись регистра в history из пакета: (router_sn, equip_type, panel_id, addr)
# → ts по часам роутера (как и метки строк history)
_last_write_ts: dict[tuple[str, str, int, int], datetime] = {}
_WRITE_TS_CACHE_WARN = 100_000

# Последняя heartbeat-строка регистра → ts по часам роутера. Отдельно от
# _last_write_ts: синтетическая строка не должна через min_interval
# подавлять следующее настоящее изменение.
_heartbeat_ts: dict[tuple[str, str, int, int], datetime] = {}

# «Устоявшаяся» метка панели: (router_sn, equip_type, panel_id) → ts роутера,
# до которой все пакеты панели уже записаны (см. _note_packet_ts)
_panel_settled_ts: dict[tuple[str, str, int], datetime] = {}

# ── Gap detection ───────────────────────────────────────────────────────────
# Трекер на уровне оборудования (router_sn, equip_type, panel_id).
# Использует device-time (ts из payload) для корректной обработки буферизации.

_last_packet_ts: dict[tuple[str, str, int], datetime] = {}
_avg_interval:   dict[tuple[str, str, int], float] = {}

# Кэш активных fault-битов: (router_sn, equip_type, panel_id, addr) → set of bit numbers
_active_fault_bits: dict[tuple[str, str, int, int], set[int]] = {}

# Кэш активных enum-состояний: (router_sn, equip_type, panel_id, addr) → value (int)
_active_enum_states: dict[tuple[str, str, int, int], int] = {}

# Столбец raw в history/latest_state — INT (int4). Значения вне диапазона
# (u32 больше 2^31-1) пишутся как NULL, иначе executemany откатывает весь пакет.
_RAW_INT4_MIN = -2**31
_RAW_INT4_MAX = 2**31 - 1

# Троттлинг WARNING о raw вне int4: (router_sn, equip_type, panel_id, addr) → datetime
_raw_overflow_warned: dict[tuple[str, str, int, int], datetime] = {}
_RAW_OVERFLOW_WARN_EVERY_SEC = 3600


class PacketDataError(Exception):
    """Ошибка данных пакета (asyncpg.DataError) с указанием регистра.

    Детерминированная: повтор того же пакета упадёт так же, ретраи не нужны.
    """


class _StateDelta:
    """Изменения in-memory состояния, накопленные за один пакет decoded.

    Применяются (apply) только после commit транзакции пакета. Если запись
    упала, состояние остаётся прежним: повтор пакета снова увидит переходы
    enum/fault как изменения и запишет открытие/закрытие периодов.
    """

    __slots__ = ("packet_ts", "settled_ts", "avg_interval", "fault_bits", "enum_states", "write_ts")

    def __init__(self) -> None:
        self.packet_ts:    dict[tuple[str, str, int], datetime] = {}
        self.settled_ts:   dict[tuple[str, str, int], datetime | None] = {}   # None — сбросить
        self.avg_interval: dict[tuple[str, str, int], float] = {}
        self.fault_bits:   dict[tuple[str, str, int, int], set[int]] = {}
        self.enum_states:  dict[tuple[str, str, int, int], int | None] = {}
        self.write_ts:     dict[tuple[str, str, int, int], datetime] = {}

    def active_fault_bits(self, key: tuple[str, str, int, int]) -> set[int]:
        if key in self.fault_bits:
            return self.fault_bits[key]
        return _active_fault_bits.get(key, set())

    def active_enum_state(self, key: tuple[str, str, int, int]) -> int | None:
        if key in self.enum_states:
            return self.enum_states[key]
        return _active_enum_states.get(key)

    def apply(self) -> None:
        _last_packet_ts.update(self.packet_ts)
        for ekey, settled in self.settled_ts.items():
            if settled is None:
                _panel_settled_ts.pop(ekey, None)
            else:
                _panel_settled_ts[ekey] = settled
        _avg_interval.update(self.avg_interval)
        _active_fault_bits.update(self.fault_bits)
        _active_enum_states.update(self.enum_states)
        for key, ts in self.write_ts.items():
            _update_last_write_ts(key, ts)

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
    """При старте загрузить _last_write_ts из history (ts — часы роутера)."""
    async with db.pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT router_sn, equip_type, panel_id, addr, max(ts) AS last_ts "
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
    """При старте — только информационный лог.

    Открытые gap'ы (gap_end IS NULL) закрываются безусловно при получении
    следующего нормального пакета для того же оборудования (см.
    _check_equipment_gap), независимо от того, помнит ли о них текущий
    процесс — поэтому отдельный in-memory трекер открытых gap'ов не нужен.
    """
    async with db.pool().acquire() as conn:
        gap_rows = await db.get_open_gaps(conn)
    logger.info("Gap tracker startup: %d open gap(s) in DB", len(gap_rows))


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch
# ─────────────────────────────────────────────────────────────────────────────

_GZIP_MAGIC = b"\x1f\x8b"


def _parse_json_payload(topic: str, payload_bytes: bytes) -> dict[str, Any] | None:
    """Распарсить JSON payload в dict.

    Неразбираемый payload — не повод для ретраев воркера: логируем и None.
    """
    try:
        data = json.loads(payload_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Bad JSON on %s: %s", topic, e)
        return None
    if not isinstance(data, dict):
        logger.warning("Payload on %s is not a JSON object", topic)
        return None
    return data


async def dispatch(topic: str, payload_bytes: bytes, cfg: AppConfig) -> None:
    """Главная точка входа: topic + raw payload → обработка."""

    # Gzip-пакеты на cg/v1/telemetry — сжатые регистровые дампы, их потребитель
    # декодер. db-writer берёт из telemetry только GPS, он приходит несжатым.
    # Признак жизни роутера не теряется: _last_seen обновляется при приёме.
    if payload_bytes[:2] == _GZIP_MAGIC:
        logger.debug("Skipping gzip payload on %s (decoder input, not ours)", topic)
        return

    # Register map (retained, обновляется при старте и при изменениях)
    m_maps = _RE_MAPS.match(topic)
    if m_maps:
        device_type = m_maps.group(1)
        data = _parse_json_payload(topic, payload_bytes)
        if data is None:
            return
        register_map.update(device_type, data)
        async with db.pool().acquire() as conn:
            await register_map.sync_to_db(conn, device_type)
        return

    m_tel = _RE_TELEMETRY.match(topic)
    if m_tel:
        router_sn = m_tel.group(1)
        data = _parse_json_payload(topic, payload_bytes)
        if data is None:
            return
        await _handle_telemetry(router_sn, data, cfg)
        return

    m_dec = _RE_DECODED.match(topic)
    if m_dec:
        router_sn  = m_dec.group(1)
        equip_type = m_dec.group(2)
        panel_id   = int(m_dec.group(3))
        data = _parse_json_payload(topic, payload_bytes)
        if data is None:
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
    delta: _StateDelta,
) -> None:
    """Проверка gap'а на уровне оборудования.

    Использует device-time (ts) для корректной обработки буферизации:
    если роутер потерял связь, накопил пакеты и выслал их пачкой,
    интервалы по device-time будут нормальными — ложного gap'а не будет.

    Новое состояние трекера пишется в delta и применяется после commit.
    """
    ekey = (router_sn, equip_type, panel_id)
    prev_ts = _last_packet_ts.get(ekey)

    if prev_ts is not None:
        elapsed = (ts - prev_ts).total_seconds()

        # Защита от отрицательного elapsed (пакеты пришли не по порядку)
        if elapsed < 0:
            delta.packet_ts[ekey] = ts
            return

        avg = _avg_interval.get(ekey)

        if avg is None:
            delta.avg_interval[ekey] = elapsed
            avg = elapsed
            is_gap = False
        else:
            threshold = max(avg * gap_cfg.multiplier, float(gap_cfg.min_threshold_sec))
            is_gap = elapsed > threshold
            if not is_gap:
                delta.avg_interval[ekey] = gap_cfg.ema_alpha * elapsed + (1 - gap_cfg.ema_alpha) * avg

        if is_gap:
            gap_id = await db.insert_data_gap(conn, router_sn, equip_type, panel_id, prev_ts)
            logger.info(
                "GAP opened id=%d %s/%s/%d: elapsed=%.0fs threshold=%.0fs avg=%.0fs",
                gap_id, router_sn, equip_type, panel_id, elapsed, threshold, avg,
            )

        # Безусловно: закрыть любой открытый gap (gap_end IS NULL) для этого
        # оборудования, включая тот, что открыт строкой выше, и любые "осиротевшие"
        # записи из прошлого (не зависит от in-memory состояния процесса).
        count = await db.close_data_gap(conn, router_sn, equip_type, panel_id, ts)
        if count:
            logger.info(
                "GAP closed %s/%s/%d: %d gap(s) closed",
                router_sn, equip_type, panel_id, count,
            )

    delta.packet_ts[ekey] = ts


def _note_packet_ts(ekey: tuple[str, str, int], ts: datetime, delta: _StateDelta) -> None:
    """Обновить «устоявшуюся» метку панели — до неё все пакеты уже записаны.

    Блоки одного цикла опроса приходят с одной меткой роутера и доходят до
    записи с разбросом: пока виден последний ts, часть его блоков может быть
    ещё в пути. Предыдущая отличная метка надёжна — пакеты панели
    обрабатываются по порядку одним воркером. Пакет «назад» (не по порядку
    или часы роутера отскочили) — сброс, метка появится со следующим циклом.
    """
    latest = _last_packet_ts.get(ekey)
    if latest is None or ts == latest:
        return
    delta.settled_ts[ekey] = latest if ts > latest else None


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

    # In-memory состояние меняем только после commit (см. _StateDelta)
    delta = _StateDelta()
    pkt = (router_sn, equip_type, panel_id)

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await db.upsert_object(conn, router_sn)
            await db.upsert_equipment(conn, router_sn, equip_type, panel_id)

            # Gap detection по device-time
            await _check_equipment_gap(
                conn, router_sn, equip_type, panel_id, ts, cfg.gap_detector, delta,
            )
            _note_packet_ts(pkt, ts, delta)

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
                            addr=addr,
                            raw_val=_raw_for_db(reg, router_sn, equip_type, panel_id, addr, now),
                            value_dict=value, ts=ts,
                            prev_map=prev_map,
                            latest_rows_map=latest_rows_map,
                            history_batch=history_batch,
                            fault_open_batch=fault_open_batch,
                            fault_close_batch=fault_close_batch,
                            event_rows=event_rows,
                            cfg=cfg,
                            delta=delta,
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
                    delta=delta,
                )

            if latest_rows_map:
                await _write_batch("latest_state", db.upsert_latest_state_batch,
                                   conn, list(latest_rows_map.values()), pkt)
            if history_batch:
                await _write_batch("history", db.insert_history_batch,
                                   conn, history_batch, pkt)
            if enum_close_batch:                            # сначала закрываем старые
                await _write_batch("enum_close", db.close_enum_states_batch,
                                   conn, enum_close_batch, pkt)
            if enum_open_batch:                             # потом открываем новые
                await _write_batch("enum_open", db.open_enum_state_batch,
                                   conn, enum_open_batch, pkt)
            if fault_close_batch:                           # сначала закрываем старые
                await _write_batch("fault_close", db.close_faults_batch,
                                   conn, fault_close_batch, pkt)
            if fault_open_batch:                            # потом открываем новые
                await _write_batch("fault_open", db.open_fault_batch,
                                   conn, fault_open_batch, pkt)
            if event_rows:
                await db.insert_event_batch(conn, event_rows)

    # Транзакция зафиксирована — теперь можно обновить in-memory состояние
    delta.apply()

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
    delta: _StateDelta,
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
    prev_bits: set[int] = delta.active_fault_bits(fkey)

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

    delta.fault_bits[fkey] = current_bits

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
    delta: _StateDelta,
) -> None:
    try:
        addr = int(reg["addr"])
    except (KeyError, TypeError, ValueError):
        logger.warning("Register without addr in %s/%d", router_sn, panel_id)
        return

    # Пропускаем регистры с ошибкой декодирования
    if reg.get("reason"):
        return

    # na=true — сервисный код «нет данных»: value=NULL, raw=NULL (прочерк в latest_state)
    value   = None if reg.get("na") else reg.get("value")
    raw_val = _raw_for_db(reg, router_sn, equip_type, panel_id, addr, now)

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
    # Интервалы — по часам роутера (ts пакета), как и метки строк history:
    # серверное время приёма пачки ничего не говорит о моменте показания
    last_ts = _last_write_ts.get(key)

    if params.register_kind == "enum":
        # ── Enum: только change, без heartbeat ───────────────────────────
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val,
            prev_value=prev_value, prev_raw=prev_raw,
            last_write_ts=last_ts, now=ts,
            use_heartbeat=False,
        )
        if decision.write:
            history_batch.append((router_sn, equip_type, panel_id, addr, ts, dec_value, raw_val))
            delta.write_ts[key] = ts

        # enum_history: открываем/закрываем периоды состояния
        ekey = (router_sn, equip_type, panel_id, addr)
        prev_enum_val = delta.active_enum_state(ekey)
        if raw_val != prev_enum_val:
            if prev_enum_val is not None:
                enum_close_batch.append((router_sn, equip_type, panel_id, addr, ts))
            if raw_val is not None:
                enum_open_batch.append((router_sn, equip_type, panel_id, addr, raw_val, ts))
            delta.enum_states[ekey] = raw_val

    else:
        # ── Аналог (default): change + tolerance + heartbeat ─────────────
        decision = should_write(
            params,
            new_value=dec_value, new_raw=raw_val,
            prev_value=prev_value, prev_raw=prev_raw,
            last_write_ts=last_ts, now=ts,
            use_heartbeat=True,
        )
        if decision.write:
            history_batch.append((router_sn, equip_type, panel_id, addr, ts, dec_value, raw_val))
            delta.write_ts[key] = ts


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


def _raw_for_db(
    reg: dict[str, Any],
    router_sn: str,
    equip_type: str,
    panel_id: int,
    addr: int,
    now: datetime,
) -> int | None:
    """raw для записи в БД (столбец raw — INT).

    - na=true (сервисный код «нет данных») → NULL: сырой код не нужен.
    - вне int4 (u32 больше 2^31-1) → NULL + WARNING (не чаще раза в час на
      регистр). Пакет при этом пишется, значение остаётся в value (NUMERIC).
    """
    if reg.get("na"):
        return None
    raw = _safe_int(reg.get("raw"))
    if raw is None or _RAW_INT4_MIN <= raw <= _RAW_INT4_MAX:
        return raw

    key = (router_sn, equip_type, panel_id, addr)
    last = _raw_overflow_warned.get(key)
    if last is None or (now - last).total_seconds() >= _RAW_OVERFLOW_WARN_EVERY_SEC:
        _raw_overflow_warned[key] = now
        logger.warning(
            "raw out of int4 range %s/%s/%d addr=%d raw=%d value=%s: writing raw=NULL",
            router_sn, equip_type, panel_id, addr, raw, reg.get("value"),
        )
    return None


_RE_EXECUTEMANY_ELEMENT = re.compile(r"element #(\d+) of executemany")


async def _write_batch(
    name: str,
    write: Callable[[asyncpg.Connection, list[tuple]], Awaitable[None]],
    conn: asyncpg.Connection,
    rows: list[tuple],
    pkt: tuple[str, str, int],
) -> None:
    """Записать батч; при ошибке данных указать регистр, на котором упало.

    rows — кортежи вида (router_sn, equip_type, panel_id, addr, ...).
    asyncpg сообщает номер строки executemany ("element #N") — по нему
    находим addr; если номера нет, перечисляем addr всего батча.
    """
    try:
        await write(conn, rows)
    except asyncpg.DataError as e:
        router_sn, equip_type, panel_id = pkt
        m = _RE_EXECUTEMANY_ELEMENT.search(str(e))
        if m and int(m.group(1)) < len(rows):
            where = f"addr={rows[int(m.group(1))][3]}"
        else:
            where = f"addrs={sorted({r[3] for r in rows})}"
        raise PacketDataError(
            f"{name} {router_sn}/{equip_type}/{panel_id} {where}: {e}"
        ) from e


def _update_last_write_ts(key: tuple, ts: datetime) -> None:
    _last_write_ts[key] = ts
    if len(_last_write_ts) > _WRITE_TS_CACHE_WARN:
        logger.warning(
            "_last_write_ts cache size=%d exceeds %d — possible memory growth",
            len(_last_write_ts), _WRITE_TS_CACHE_WARN,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Timer-driven heartbeat
# ─────────────────────────────────────────────────────────────────────────────
# Роутер не публикует регистр, пока его значение не меняется (экономия
# трафика). Событийный heartbeat в _process_register в этом случае не
# срабатывает: нет сообщения — нет записи. Пока панель живая (пакеты
# приходят), дописываем последнее известное значение аналоговых регистров.
#
# Метка строки — «устоявшаяся» метка пакетов панели (часы роутера), а не
# время сервера: если от панели пришли пакеты до момента T без регистра,
# его значение держалось до T. Пачки доходят с задержкой 1–25 с, и метка
# сервера обгоняла бы ещё не доставленные показания — на пуске в history
# появлялся «0 со стоянки» между рабочими значениями.

async def heartbeat_flush_loop(
    cfg: AppConfig,
    panel_last_seen: dict[tuple[str, str, int], datetime],
) -> None:
    hp = cfg.history_policy
    scan_sec = max(5, hp.heartbeat_scan_sec)
    kpi_map  = hp.kpi_map()
    logger.info(
        "Heartbeat flush loop started (scan=%ds, source_alive=%ds)",
        scan_sec, hp.heartbeat_source_alive_sec,
    )
    while True:
        await asyncio.sleep(scan_sec)
        try:
            await _heartbeat_flush_once(cfg, kpi_map, panel_last_seen)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Heartbeat flush failed")


async def _heartbeat_flush_once(
    cfg: AppConfig,
    kpi_map: dict[tuple[str, int], Any],
    panel_last_seen: dict[tuple[str, str, int], datetime],
) -> None:
    now = datetime.now(timezone.utc)
    alive_sec = cfg.history_policy.heartbeat_source_alive_sec

    # Живые панели (приём по часам сервера) → устоявшаяся метка (часы роутера)
    settled: dict[tuple[str, str, int], datetime] = {}
    for pkey, seen in panel_last_seen.items():
        if (now - seen).total_seconds() > alive_sec:
            continue
        pts = _panel_settled_ts.get(pkey)
        if pts is not None:
            settled[pkey] = pts
    if not settled:
        return

    # Аналоговые регистры, у которых к устоявшейся метке панели прошло
    # heartbeat_sec с последней строки в history (всё — по часам роутера)
    stale: dict[tuple[str, str, int], list[int]] = {}
    for key, wts in _last_write_ts.items():
        pkey = key[:3]
        pts = settled.get(pkey)
        if pts is None:
            continue
        sn, et, pid, addr = key
        params = resolve_params(cfg, et, addr, register_map.get_unit(et, addr), kpi_map)
        if params.register_kind != "analog" or not params.store_history:
            continue
        last = max(wts, _heartbeat_ts.get(key, wts))
        if (pts - last).total_seconds() < params.heartbeat_sec:
            continue
        stale.setdefault(pkey, []).append(addr)

    if not stale:
        return

    written = 0
    async with db.pool().acquire() as conn:
        for pkey, addrs in stale.items():
            sn, et, pid = pkey
            pts = settled[pkey]
            prev_map = await db.get_latest_state_rows_many(conn, sn, et, pid, addrs)
            batch: list[tuple] = []
            for addr in addrs:
                prev = prev_map.get(addr)
                if prev is None or prev["value"] is None:
                    continue
                # Значение снято позже метки — неизвестно, каким оно было на ней
                if prev["ts"] is None or prev["ts"] > pts:
                    continue
                batch.append((sn, et, pid, addr, pts, prev["value"], prev["raw"]))
            await db.insert_history_batch(conn, batch)
            # Метка heartbeat — только после успешной вставки
            for row in batch:
                _heartbeat_ts[row[:4]] = pts
            written += len(batch)

    if written:
        logger.debug("Heartbeat flush: %d stale register(s) written", written)
