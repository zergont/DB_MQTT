# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""CG DB-Writer — логика решения «писать ли в history».

Тип регистра определяется из in-memory register map (cg/v1/maps/+),
а не из register_catalog БД.

Routing:
  analog      → history, change + tolerance + heartbeat
  enum        → history, только change (без heartbeat)
  fault_bitmap → history (raw), только change; fault_history — отдельно
  unknown     → analog (defaults)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from src.config import AppConfig


@dataclass
class _RegParams:
    """Эффективные параметры для конкретного регистра."""
    tolerance:       float | None
    min_interval_sec: int
    heartbeat_sec:   int
    store_history:   bool
    register_kind:   str   # analog | enum | fault_bitmap


def resolve_params(
    cfg: AppConfig,
    equip_type: str,
    addr: int,
    map_unit: str | None,
    kpi_map: dict[tuple[str, int], Any],
) -> _RegParams:
    """Определить параметры записи для регистра.

    map_unit — значение поля 'unit' из in-memory register map.
    Если None (регистр не в карте) — считаем аналогом.

    kpi_map передаётся снаружи (вычислен один раз на сообщение).
    """
    d = cfg.history_policy.defaults

    tolerance     = d.tolerance_analog
    min_interval  = d.min_interval_sec
    heartbeat     = d.heartbeat_sec
    store         = d.store_history

    # Тип регистра по unit из map
    if map_unit == "enum":
        register_kind = "enum"
        tolerance     = None   # для enum tolerance не применяется
    elif map_unit == "fault_bitmap":
        register_kind = "fault_bitmap"
        tolerance     = None
    else:
        register_kind = "analog"

    # KPI-регистры перекрывают defaults
    kpi = kpi_map.get((equip_type, addr))
    if kpi is not None:
        min_interval = kpi.min_interval_sec
        heartbeat    = kpi.heartbeat_sec
        tolerance    = kpi.tolerance

    return _RegParams(
        tolerance=tolerance,
        min_interval_sec=min_interval,
        heartbeat_sec=heartbeat,
        store_history=store,
        register_kind=register_kind,
    )


@dataclass
class WriteDecision:
    write: bool
    write_reason: str = ""


def should_write(
    params: _RegParams,
    *,
    new_value: Decimal | None,
    new_raw: int | None,
    prev_value: Decimal | None,
    prev_raw: int | None,
    last_write_ts: datetime | None,
    now: datetime,
    use_heartbeat: bool = True,
) -> WriteDecision:
    """Решает, нужно ли писать запись в history.

    analog:       use_heartbeat=True  (change + tolerance + heartbeat)
    enum:         use_heartbeat=False (только change)
    fault_bitmap: use_heartbeat=False (только change по raw)
    """
    if not params.store_history:
        return WriteDecision(False)

    elapsed: float | None = None
    if last_write_ts is not None:
        elapsed = (now - last_write_ts).total_seconds()

    # Минимальный интервал — защита от flood
    if elapsed is not None and elapsed < params.min_interval_sec:
        return WriteDecision(False)

    # Детектирование изменения по raw (точный целочисленный сигнал)
    changed = new_raw != prev_raw

    # Для аналогов — дополнительно проверяем tolerance по float-значению
    if not changed and params.tolerance is not None:
        try:
            nv = float(new_value) if new_value is not None else None
            pv = float(prev_value) if prev_value is not None else None
            if nv is not None and pv is not None:
                if abs(nv - pv) > params.tolerance:
                    changed = True
            elif nv != pv:
                changed = True
        except (TypeError, ValueError):
            pass

    if changed:
        return WriteDecision(True, "change")

    # Heartbeat — только для аналогов, для непрерывности графика
    if use_heartbeat and (elapsed is None or elapsed >= params.heartbeat_sec):
        return WriteDecision(True, "heartbeat")

    return WriteDecision(False)
