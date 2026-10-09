# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""CG DB-Writer — in-memory register map из cg/v1/maps/+ MQTT-топика.

Топик retained, публикуется telemetry2 при старте один раз на device_type.
db_writer подписывается на cg/v1/maps/+ и хранит карту в памяти.
Карта используется вместо DB-запросов к register_catalog при обработке
decoded-сообщений.

Формат топика: cg/v1/maps/<device_type>
Формат payload:
{
  "device_type": "pcc",
  "registers": {
    "40736": {"name": "...", "unit": "kPa", "notes_ru": "..."},
    "40010": {"name": "...", "unit": "enum", "labels": {...}},
    "40400": {"name": "...", "unit": "fault_bitmap", "bits": {
        "0": {"name": "LowOilPressureShutdown", "severity": "shutdown"},
        ...
    }},
    "input:30013": {"name": "...", "unit": "pcs"}
  }
}

Ключ holding-регистра — число, input-регистра — с префиксом "input:".
В decoded addr приходит без префикса (input — 30000+смещение), поэтому
в карте db-writer ключ — просто addr.
"""

from __future__ import annotations

import json as _json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger("cg.register_map")

# device_type → {addr(int) → {unit, bits, labels, ...}}
_maps: dict[str, dict[int, dict[str, Any]]] = {}


_KEY_REG_TYPES = ("holding", "input")


def _parse_addr_key(key: Any) -> int | None:
    """Ключ карты ("40070", "input:30013", "holding:40070") → addr или None."""
    reg_type, sep, addr_str = str(key).partition(":")
    if not sep:
        reg_type, addr_str = "holding", reg_type
    if reg_type not in _KEY_REG_TYPES:
        return None
    try:
        return int(addr_str)
    except ValueError:
        return None


def update(device_type: str, payload: dict[str, Any]) -> None:
    """Обновить карту из распарсенного payload cg/v1/maps/<device_type>."""
    registers_raw = payload.get("registers") or {}
    registers: dict[int, dict[str, Any]] = {}
    source_keys: dict[int, str] = {}
    for key, meta in registers_raw.items():
        addr = _parse_addr_key(key)
        if addr is None:
            logger.warning("register_map: bad addr key %r in %s", key, device_type)
            continue
        if addr in registers:
            # input и holding с одним номером в decoded неразличимы
            logger.warning(
                "register_map: addr %d in %s: key %r collides with %r, keeping %r",
                addr, device_type, key, source_keys[addr], source_keys[addr],
            )
            continue
        registers[addr] = meta
        source_keys[addr] = key

    _maps[device_type] = registers
    logger.info(
        "Register map updated: device_type=%s registers=%d",
        device_type, len(registers),
    )


def get_unit(equip_type: str, addr: int) -> str | None:
    """Вернуть unit для регистра или None если регистр не в карте."""
    m = _maps.get(equip_type)
    if m is None:
        return None
    entry = m.get(addr)
    if entry is None:
        return None
    return entry.get("unit")


def get_entry(equip_type: str, addr: int) -> dict[str, Any] | None:
    """Вернуть полную запись карты для регистра или None."""
    m = _maps.get(equip_type)
    if m is None:
        return None
    return m.get(addr)


def is_loaded(equip_type: str) -> bool:
    """True если карта для device_type уже получена."""
    return equip_type in _maps


async def sync_to_db(conn: "asyncpg.Connection", equip_type: str) -> None:
    """UPSERT register_catalog из текущего in-memory map для equip_type.

    Вызывается после update() при получении cg/v1/maps/<device_type>.
    Позволяет потребителям БД получать имена/единицы без подписки на MQTT.
    """
    entries = _maps.get(equip_type, {})
    if not entries:
        return

    rows = []
    for addr, entry in entries.items():
        unit = entry.get("unit") or ""
        if unit == "fault_bitmap":
            kind = "fault_bitmap"
            states = entry.get("bits")          # {"0": {"name": ..., "name_ru": ..., "severity": ...}}
        elif unit == "enum":
            kind = "enum"
            labels    = entry.get("labels")     # {"0": "Off", "1": "Auto"}
            labels_ru = entry.get("labels_ru")  # {"0": "Выкл", "1": "Авто"} — опционально
            states = {"labels": labels}
            if labels_ru:
                states["labels_ru"] = labels_ru
        else:
            kind = "analog"
            states = None

        states_json = _json.dumps(states, ensure_ascii=False) if states else None
        rows.append((
            equip_type,
            addr,
            entry.get("name"),
            entry.get("notes_ru") or None,
            unit or None,
            kind,
            states_json,
        ))

    await conn.executemany(
        """
        INSERT INTO register_catalog
          (equip_type, addr, name_default, name_ru, unit_default, register_kind, states_json)
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        ON CONFLICT (equip_type, addr) DO UPDATE SET
          name_default  = EXCLUDED.name_default,
          name_ru       = EXCLUDED.name_ru,
          unit_default  = EXCLUDED.unit_default,
          register_kind = EXCLUDED.register_kind,
          states_json   = EXCLUDED.states_json
        """,
        rows,
    )
    logger.info(
        "register_catalog synced: equip_type=%s rows=%d",
        equip_type, len(rows),
    )
