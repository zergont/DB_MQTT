# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""Таймерный heartbeat: метки по часам роутера, без «0 со стоянки» на пуске.

Сценарий из жизни (ДЭС №3, 13.10): машина стоит, обороты 0 не меняются и
роутер их не шлёт — heartbeat повторяет 0. На пуске блоки цикла опроса
приходят с одной меткой и с задержкой; heartbeat не должен вставить 0
после (или в один момент с) рабочим показанием.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src import handlers
from src.config import AppConfig, KpiRegister
from src.history_policy import resolve_params, should_write
from tests.test_decoded_write import ET, PID, SN, DecodedTestCase, packet, reg

RPM = 40068        # обороты, KPI: heartbeat 60 с
OIL = 40062        # давление масла, KPI
COUNTER = 40290    # время работы контроллера — меняется каждый цикл
FUEL_P = 40258     # обычный аналог (defaults)

BASE = datetime(2026, 10, 13, 11, 58, 0, tzinfo=timezone.utc)
PANEL = (SN, ET, PID)


def at(sec: float) -> datetime:
    return BASE + timedelta(seconds=sec)


class HeartbeatTestCase(DecodedTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.cfg.history_policy.kpi_registers = [
            KpiRegister(addr=RPM, heartbeat_sec=60, tolerance=0.1),
            KpiRegister(addr=OIL, heartbeat_sec=60, tolerance=0.1),
        ]
        self.cfg.history_policy.defaults.heartbeat_sec = 60
        self.seen: dict = {}

    async def pkt(self, sec: float, *regs: dict) -> None:
        """Пакет панели с меткой роутера BASE+sec; панель «живая» по часам сервера."""
        await self.handle(packet(*regs, ts=at(sec).isoformat()))
        self.seen[PANEL] = datetime.now(timezone.utc)

    async def counter(self, sec: float) -> None:
        await self.pkt(sec, reg(COUNTER, 1000 + sec, int(1000 + sec)))

    async def flush(self) -> None:
        await handlers._heartbeat_flush_once(
            self.cfg, self.cfg.history_policy.kpi_map(), self.seen,
        )

    def series(self, addr: int) -> list[tuple[float, Decimal | None]]:
        return [((ts - BASE).total_seconds(), v) for ts, v in self.fake.series(addr)]


class StartupRaceTests(HeartbeatTestCase):

    async def test_no_stale_zero_after_start(self):
        await self.pkt(0, reg(RPM, 0, 0), reg(OIL, 0, 0), reg(COUNTER, 1000, 1000))
        for sec in range(9, 64, 9):                  # стоянка: шлётся только счётчик
            await self.counter(sec)

        # Цикл пуска, метка 72: блок счётчика уже записан, блок с оборотами в пути
        await self.counter(72)
        await self.flush()                           # heartbeat срабатывает в этот момент
        await self.pkt(72, reg(RPM, 1515, 1515), reg(OIL, 300, 300))
        await self.pkt(81, reg(RPM, 1496, 1496), reg(OIL, 316, 316))
        await self.flush()

        for addr, running in ((RPM, [1515, 1496]), (OIL, [300, 316])):
            with self.subTest(addr=addr):
                s = self.series(addr)
                # 0 повторён на устоявшейся метке 63 — до пуска, а не после
                self.assertEqual(s, [(0, 0), (63, 0), (72, running[0]), (81, running[1])])

    async def test_heartbeat_ts_is_router_time(self):
        await self.pkt(0, reg(RPM, 0, 0))
        for sec in range(9, 73, 9):
            await self.counter(sec)
        await self.flush()

        rows = [h for h in self.fake.history_for(RPM) if h[3] != BASE]
        self.assertEqual([(h[3], h[1]) for h in rows], [(at(63), 0)])
        self.assertEqual(handlers._heartbeat_ts[(SN, ET, PID, RPM)], at(63))


class StalenessTests(HeartbeatTestCase):

    async def test_staleness_measured_by_router_clock(self):
        await self.pkt(0, reg(RPM, 0, 0))
        for sec in range(9, 46, 9):                  # устоялось 36 с по роутеру
            await self.counter(sec)
        await self.flush()                           # сервер давно «позже», но 36 < 60
        self.assertEqual(self.series(RPM), [(0, 0)])

        for sec in range(54, 73, 9):                 # устоялось 63
            await self.counter(sec)
        await self.flush()
        await self.flush()                           # новых пакетов нет — повтора нет
        self.assertEqual(self.series(RPM), [(0, 0), (63, 0)])

        for sec in range(81, 128, 9):                # устоялось 117: 117 − 63 < 60
            await self.counter(sec)
        await self.flush()
        self.assertEqual(len(self.series(RPM)), 2)

        await self.counter(135)                      # устоялось 126: 126 − 63 ≥ 60
        await self.flush()
        self.assertEqual(self.series(RPM), [(0, 0), (63, 0), (126, 0)])

    async def test_no_heartbeat_without_settled_ts(self):
        await self.pkt(0, reg(RPM, 0, 0))            # один пакет — устоявшейся метки нет
        await self.flush()
        self.assertEqual(self.series(RPM), [(0, 0)])

    async def test_skip_when_value_observed_after_settled_ts(self):
        await self.pkt(0, reg(RPM, 0, 0))
        for sec in range(9, 73, 9):
            await self.counter(sec)
        self.fake.t["latest_ts"][RPM] = at(72)       # значение снято позже метки 63
        await self.flush()
        self.assertEqual(self.series(RPM), [(0, 0)])

    async def test_dead_panel_no_heartbeat(self):
        await self.pkt(0, reg(RPM, 0, 0))
        for sec in range(9, 73, 9):
            await self.counter(sec)
        self.seen[PANEL] = datetime.now(timezone.utc) - timedelta(hours=1)
        await self.flush()
        self.assertEqual(self.series(RPM), [(0, 0)])


class HeartbeatVsRealWriteTests(HeartbeatTestCase):

    async def test_heartbeat_does_not_suppress_next_change(self):
        # min_interval 2 с: heartbeat на 70 не должен глушить изменение на 71
        self.cfg.history_policy.defaults.min_interval_sec = 2
        await self.pkt(0, reg(FUEL_P, 278.7, 2787))
        for sec in (9, 18, 27, 36, 45, 54, 63, 70, 71):
            await self.counter(sec)                  # устоялось 70
        await self.flush()                           # heartbeat FUEL_P на 70
        await self.pkt(71, reg(FUEL_P, 280.4, 2804))  # второй блок цикла 71

        self.assertEqual(
            self.series(FUEL_P),
            [(0, Decimal("278.7")), (70, Decimal("278.7")), (71, Decimal("280.4"))],
        )

    async def test_real_write_resets_heartbeat_cadence(self):
        await self.pkt(0, reg(RPM, 0, 0))
        for sec in range(9, 73, 9):
            await self.counter(sec)
        await self.flush()                           # heartbeat на 63
        await self.pkt(75, reg(RPM, 1500, 1500))     # настоящее показание
        for sec in range(81, 136, 9):                # устоялось 126: 126 − 75 < 60
            await self.counter(sec)
        await self.flush()
        self.assertEqual(self.series(RPM), [(0, 0), (63, 0), (75, 1500)])

    async def test_event_write_ts_is_router_time(self):
        await self.pkt(0, reg(RPM, 0, 0))
        self.assertEqual(handlers._last_write_ts[(SN, ET, PID, RPM)], BASE)


class SettledTsTests(HeartbeatTestCase):

    async def test_settled_is_previous_distinct_ts(self):
        await self.counter(0)
        self.assertNotIn(PANEL, handlers._panel_settled_ts)
        await self.counter(9)
        await self.pkt(9, reg(RPM, 0, 0))            # второй блок с той же меткой
        self.assertEqual(handlers._panel_settled_ts[PANEL], at(0))
        await self.counter(18)
        self.assertEqual(handlers._panel_settled_ts[PANEL], at(9))

    async def test_backward_ts_resets_settled(self):
        await self.counter(0)
        await self.counter(9)
        await self.counter(5)                        # пакет «назад»
        self.assertNotIn(PANEL, handlers._panel_settled_ts)
        await self.counter(18)
        self.assertEqual(handlers._panel_settled_ts[PANEL], at(5))

    async def test_settled_not_advanced_on_failed_write(self):
        await self.counter(0)
        await self.counter(9)
        self.fake.fail_once.add("insert_history_batch")
        with self.assertRaises(ConnectionResetError):
            await self.counter(18)
        self.assertEqual(handlers._panel_settled_ts[PANEL], at(0))


class ShouldWriteRouterClockTests(unittest.TestCase):

    def test_backward_clock_does_not_freeze_writes(self):
        params = resolve_params(AppConfig(), ET, FUEL_P, None, {})   # min_interval 2 с
        d = should_write(
            params,
            new_value=Decimal("1"), new_raw=1, prev_value=Decimal("1"), prev_raw=1,
            last_write_ts=at(3600), now=at(0),       # часы роутера ушли на час назад
        )
        self.assertTrue(d.write)


if __name__ == "__main__":
    unittest.main()
