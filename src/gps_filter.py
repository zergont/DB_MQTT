"""CG DB-Writer v2.1.0 — GPS anti-teleport фильтр."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime

from src.config import GpsFilterCfg

logger = logging.getLogger("cg.gps")

EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние между двумя точками в метрах (формула Haversine)."""
    r_lat1, r_lat2 = math.radians(lat1), math.radians(lat2)
    d_lat = r_lat2 - r_lat1
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(r_lat1) * math.cos(r_lat2) * math.sin(d_lon / 2) ** 2
    )
    return EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@dataclass
class GpsPoint:
    lat: float
    lon: float
    satellites: int | None
    fix_status: int | None
    gps_time: datetime | None
    received_at: datetime


@dataclass
class GpsVerdict:
    accepted: bool
    reject_reason: str | None = None


class GpsFilter:
    """Анти-скачок фильтр. Один экземпляр на router_sn."""

    def __init__(self, cfg: GpsFilterCfg) -> None:
        self._cfg = cfg
        self._last_accepted: GpsPoint | None = None
        self._confirm_points: list[GpsPoint] = []

    @property
    def last_accepted(self) -> GpsPoint | None:
        return self._last_accepted

    def set_initial(self, pt: GpsPoint) -> None:
        """Установить начальную принятую точку (из БД при старте)."""
        self._last_accepted = pt

    # ──────────────────────────────────────────────────────────────────────

    def check(self, pt: GpsPoint) -> GpsVerdict:
        cfg = self._cfg

        # 1) Quality gate
        if pt.satellites is not None and pt.satellites < cfg.sats_min:
            self._confirm_points.clear()
            return GpsVerdict(False, "low_sats")
        if pt.fix_status is not None and pt.fix_status < cfg.fix_min:
            self._confirm_points.clear()
            return GpsVerdict(False, "bad_fix")

        # Первая точка — принимаем без проверок
        if self._last_accepted is None:
            self._accept(pt)
            return GpsVerdict(True)

        dist = haversine_m(
            self._last_accepted.lat, self._last_accepted.lon,
            pt.lat, pt.lon,
        )

        # 2) Deadband — точка близко к последней принятой
        if dist < cfg.deadband_m:
            self._confirm_points.clear()
            return GpsVerdict(True)

        # 3) Jump gate
        dt_sec = (pt.received_at - self._last_accepted.received_at).total_seconds()
        if dt_sec <= 0:
            dt_sec = 1.0

        if dist > cfg.max_jump_m:
            return self._try_confirm(pt, dist, "jump_distance")

        speed_kmh = (dist / dt_sec) * 3.6
        if speed_kmh > cfg.max_speed_kmh:
            return self._try_confirm(pt, dist, "jump_speed")

        # Нормальная точка
        self._accept(pt)
        return GpsVerdict(True)

    # ──────────────────────────────────────────────────────────────────────

    def _accept(self, pt: GpsPoint) -> None:
        self._last_accepted = pt
        self._confirm_points.clear()

    def _try_confirm(self, pt: GpsPoint, dist: float, reason: str) -> GpsVerdict:
        """Буфер подтверждения переезда: N точек в радиусе confirm_radius_m."""
        cfg = self._cfg
        buf = self._confirm_points

        if buf:
            d_to_ref = haversine_m(buf[0].lat, buf[0].lon, pt.lat, pt.lon)
            if d_to_ref > cfg.confirm_radius_m:
                # Другой выброс — сбрасываем буфер
                self._confirm_points = [pt]
                return GpsVerdict(False, reason)
            buf.append(pt)
        else:
            buf.append(pt)

        if len(buf) >= cfg.confirm_points:
            logger.info(
                "GPS confirm move: %d points in radius %.0f m, dist=%.0f m",
                len(buf), cfg.confirm_radius_m, dist,
            )
            self._accept(pt)
            return GpsVerdict(True)

        return GpsVerdict(False, reason)
