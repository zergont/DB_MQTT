"""CG DB-Writer v2.0.0 — логика решения «писать ли в history / state_events»."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.config import AppConfig


@dataclass
class _RegParams:
    """Эффективные параметры для конкретного регистра."""
    tolerance: float | None
    min_interval_sec: int
    heartbeat_sec: int
    store_history: bool
    value_kind: str    # analog | discrete | enum | parameter | …
    register_kind: str # analog | discrete | enum | parameter


def resolve_params(
    cfg: AppConfig,
    equip_type: str,
    addr: int,
    catalog_row: Any | None,
    kpi_map: dict[tuple[str, int], Any],
) -> _RegParams:
    """Определить параметры для addr: catalog → kpi → defaults.

    kpi_map передаётся снаружи (вычислен один раз на сообщение).
    """
    d = cfg.history_policy.defaults

    tolerance    = d.tolerance_analog
    min_interval = d.min_interval_sec
    heartbeat    = d.heartbeat_sec
    store        = d.store_history
    vk           = d.value_kind
    rk           = "analog"   # register_kind default

    # Перекрываем из register_catalog
    if catalog_row is not None:
        if catalog_row["tolerance"] is not None:
            tolerance = float(catalog_row["tolerance"])
        if catalog_row["min_interval_sec"] is not None:
            min_interval = int(catalog_row["min_interval_sec"])
        if catalog_row["heartbeat_sec"] is not None:
            heartbeat = int(catalog_row["heartbeat_sec"])
        store = bool(catalog_row["store_history"])
        vk    = catalog_row["value_kind"] or vk
        rk    = catalog_row["register_kind"] or rk

    # Перекрываем из kpi_registers
    kpi = kpi_map.get((equip_type, addr))
    if kpi is not None:
        min_interval = kpi.min_interval_sec
        heartbeat    = kpi.heartbeat_sec
        tolerance    = kpi.tolerance

    # Для дискретных/enum/text — tolerance не применяется
    if vk in ("discrete", "enum", "text"):
        tolerance = None

    return _RegParams(
        tolerance=tolerance,
        min_interval_sec=min_interval,
        heartbeat_sec=heartbeat,
        store_history=store,
        value_kind=vk,
        register_kind=rk,
    )


@dataclass
class WriteDecision:
    write: bool
    write_reason: str = ""


def should_write(
    params: _RegParams,
    *,
    new_value: Any,
    new_raw: int | None,
    new_text: str | None,
    new_reason: str | None,
    prev_value: Any,
    prev_raw: int | None,
    prev_text: str | None,
    prev_reason: str | None,
    last_write_ts: datetime | None,
    now: datetime,
    use_heartbeat: bool = True,
) -> WriteDecision:
    """Решает, нужно ли писать запись.

    Используется для:
      - аналоговых регистров (history):          use_heartbeat=True
      - дискретных/enum регистров (state_events): use_heartbeat=True
      - параметров (parameter_history):           use_heartbeat=False
    """
    if not params.store_history:
        return WriteDecision(False)

    elapsed: float | None = None
    if last_write_ts is not None:
        elapsed = (now - last_write_ts).total_seconds()

    # Минимальный интервал — защита от flood
    if elapsed is not None and elapsed < params.min_interval_sec:
        return WriteDecision(False)

    # Детектирование изменения
    changed = False
    if new_raw != prev_raw:
        changed = True
    if new_text != prev_text:
        changed = True
    if new_reason != prev_reason:
        changed = True

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

    # Heartbeat — запись для детекции gap'ов
    if use_heartbeat and (elapsed is None or elapsed >= params.heartbeat_sec):
        return WriteDecision(True, "heartbeat")

    return WriteDecision(False)
