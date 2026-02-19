"""
CG DB-Writer — логика решения «писать ли в history».
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.config import AppConfig, KpiRegister, HistoryDefaults

logger = logging.getLogger("cg.history")


@dataclass
class _RegParams:
    """Эффективные параметры для конкретного регистра."""
    tolerance: float | None
    min_interval_sec: int
    heartbeat_sec: int
    store_history: bool
    value_kind: str  # analog | discrete | …


def resolve_params(
    cfg: AppConfig,
    addr: int,
    catalog_row: Any | None,
) -> _RegParams:
    """Определить параметры для addr: catalog → kpi → defaults."""
    d = cfg.history_policy.defaults
    kpi_map = cfg.history_policy.kpi_map()

    # Начинаем с дефолтов
    tolerance: float | None = d.tolerance_analog
    min_interval = d.min_interval_sec
    heartbeat = d.heartbeat_sec
    store = d.store_history
    vk = d.value_kind

    # Перекрываем из register_catalog (если есть)
    if catalog_row is not None:
        if catalog_row["tolerance"] is not None:
            tolerance = float(catalog_row["tolerance"])
        if catalog_row["min_interval_sec"] is not None:
            min_interval = int(catalog_row["min_interval_sec"])
        if catalog_row["heartbeat_sec"] is not None:
            heartbeat = int(catalog_row["heartbeat_sec"])
        store = bool(catalog_row["store_history"])
        vk = catalog_row["value_kind"] or vk

    # Перекрываем из kpi_registers
    kpi = kpi_map.get(addr)
    if kpi is not None:
        heartbeat = kpi.heartbeat_sec
        tolerance = kpi.tolerance

    # Для дискретных/enum/text — tolerance не применяется
    if vk in ("discrete", "enum", "text"):
        tolerance = None

    return _RegParams(
        tolerance=tolerance,
        min_interval_sec=min_interval,
        heartbeat_sec=heartbeat,
        store_history=store,
        value_kind=vk,
    )


@dataclass
class HistoryDecision:
    write: bool
    write_reason: str = ""


def should_write_history(
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
    last_history_ts: datetime | None,
    now: datetime,
) -> HistoryDecision:
    """Решает, нужно ли писать запись в history."""

    if not params.store_history:
        return HistoryDecision(False)

    elapsed: float | None = None
    if last_history_ts is not None:
        elapsed = (now - last_history_ts).total_seconds()

    # B) min_interval — даже если есть изменение, не чаще
    if elapsed is not None and elapsed < params.min_interval_sec:
        return HistoryDecision(False)

    # A) Change rule
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
                # Одно None, другое нет
                changed = True
        except (TypeError, ValueError):
            pass

    if changed:
        return HistoryDecision(True, "change")

    # C) Heartbeat rule
    if elapsed is None or elapsed >= params.heartbeat_sec:
        return HistoryDecision(True, "heartbeat")

    return HistoryDecision(False)
