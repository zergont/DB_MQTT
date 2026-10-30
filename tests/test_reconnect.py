# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""После обрыва связи: heartbeat, запоздалые пачки (ts_status=late), разрывы.

Случай ДЭС №3, 24.10: связи не было с 05:48:15 до 12:38:49. Первой в 12:38:28
дошла застрявшая пачка медленной группы с меткой 11:48:09 (late, значения
доаварийные), с 12:38:58 — живые пакеты. Медленные записи роутер шлёт только
при изменении, снимка после связи нет.
- heartbeat в 12:38:59, 12:54:07, 13:09:08 повторял рабочие значения
  со стоянки (снятые до обрыва) со свежей меткой;
- data_gaps разбился застрявшей пачкой на два: 615 и 616.

Здесь время сжато: обрыв после DOWN, застрявшая пачка STUCK, связь UP.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

from src import db, handlers
from tests.test_decoded_write import ADDR_ENUM, ADDR_FAULT, ET, PID, SN, packet, reg
from tests.test_gap_restore import GapRestoreTestCase
from tests.test_heartbeat import FUEL_P, PANEL, RPM, at

SLOW = 40595        # медленная группа, шлётся только при изменении (ДЭС №3: 49,9)

DOWN = 90           # последний пакет до обрыва
STUCK = 600         # метка застрявшей пачки внутри обрыва
UP = 1000           # первый живой пакет после связи


class ReconnectTestCase(GapRestoreTestCase):

    async def late(self, sec: float, *regs: dict) -> None:
        """Запоздалая пачка: декодер пометил её ts_status=late."""
        await self.handle({**packet(*regs, ts=at(sec).isoformat()), "ts_status": "late"})
        self.seen[PANEL] = datetime.now(timezone.utc)

    async def counters(self, start: float, end: float) -> None:
        for sec in range(int(start), int(end) + 1, 9):
            await self.counter(sec)

    async def before_outage(self) -> None:
        await self.pkt(0, reg(FUEL_P, 278.7, 2787), reg(SLOW, 49.9, 499), reg(RPM, 0, 0))
        await self.counters(9, DOWN)


# ─────────────────────────────────────────────────────────────────────────────
# Heartbeat не переносит значение через разрыв
# ─────────────────────────────────────────────────────────────────────────────

class StaleHeartbeatAfterGapTests(ReconnectTestCase):

    async def test_value_from_before_gap_not_repeated(self):
        await self.before_outage()
        await self.flush()                           # до обрыва heartbeat работает: 81
        await self.pkt(UP, reg(RPM, 0, 0))           # после связи пришли только обороты
        await self.counters(UP + 9, UP + 90)
        await self.flush()

        fuel = Decimal("278.7")
        self.assertEqual(self.series(FUEL_P), [(0, fuel), (81, fuel)])
        self.assertEqual(self.series(SLOW), [(0, Decimal("49.9")), (81, Decimal("49.9"))])
        # подтверждённое после связи значение повторяется как обычно
        self.assertEqual(self.series(RPM), [(0, 0), (81, 0), (UP, 0), (UP + 81, 0)])

    async def test_unchanged_value_without_gap_keeps_heartbeat(self):
        # Не ограничение по возрасту: значение 0-й секунды повторяется и через 5 мин
        await self.pkt(0, reg(FUEL_P, 278.7, 2787))
        for sec in range(9, 361, 9):
            await self.counter(sec)
            await self.flush()
        self.assertEqual([s for s, _ in self.series(FUEL_P)], [0, 63, 126, 189, 252, 315])

    async def test_heartbeat_resumes_when_value_comes_again(self):
        await self.before_outage()
        await self.pkt(UP, reg(FUEL_P, 278.7, 2787))     # пришло заново, без изменения
        await self.counters(UP + 9, UP + 90)
        await self.flush()

        fuel = Decimal("278.7")
        self.assertEqual(self.series(FUEL_P), [(0, fuel), (UP, fuel), (UP + 81, fuel)])
        self.assertEqual(self.series(SLOW), [(0, Decimal("49.9"))])

    async def test_gap_end_survives_restart(self):
        await self.before_outage()
        await self.pkt(UP, reg(RPM, 0, 0))
        await self.counters(UP + 9, UP + 45)
        await self.restart()                         # конец разрыва — из data_gaps

        await self.counters(UP + 54, UP + 90)
        await self.flush()
        self.assertEqual(self.series(FUEL_P), [(0, Decimal("278.7"))])
        self.assertEqual(handlers._panel_gap_end[PANEL], at(UP))


# ─────────────────────────────────────────────────────────────────────────────
# Запоздалая пачка внутри обрыва не делит разрыв
# ─────────────────────────────────────────────────────────────────────────────

class LatePacketGapTests(ReconnectTestCase):

    async def test_single_late_packet_does_not_split_gap(self):
        await self.before_outage()
        await self.late(STUCK, reg(SLOW, 49.9, 499))
        self.assertEqual(self.gaps(), [])                # разрыв не закрывает
        await self.counter(UP)
        await self.counter(UP + 9)
        self.assertEqual(self.gaps(), [(DOWN, UP)])
        self.assertEqual(handlers._panel_gap_end[PANEL], at(UP))

    async def test_late_cycle_of_several_blocks(self):
        await self.before_outage()
        await self.late(STUCK, reg(SLOW, 49.9, 499))
        await self.late(STUCK, reg(FUEL_P, 278.7, 2787))     # второй блок того же цикла
        await self.counter(UP)
        self.assertEqual(self.gaps(), [(DOWN, UP)])

    async def test_live_packet_inside_gap_splits_it(self):
        await self.before_outage()
        await self.counter(STUCK)                    # живой пакет — связь на миг была
        await self.counter(UP)
        self.assertEqual(self.gaps(), [(DOWN, STUCK), (STUCK, UP)])

    async def test_late_resend_written_but_gap_kept(self):
        # Досылка буфера (вся late): строки в history есть, но разрыв — от
        # последней живой записи; трекер поздние записи не двигают
        await self.before_outage()
        for sec in range(DOWN + 9, 300, 9):
            await self.late(sec, reg(SLOW, 49.9, 499))
        await self.counter(UP)
        self.assertEqual(self.gaps(), [(DOWN, UP)])
        self.assertEqual(len(self.series(SLOW)), 1 + 23)

    async def test_late_record_does_not_move_gap_start_back(self):
        # Разрыв 532: начат на метке поздней записи 18:03:01, хотя живые
        # строки шли до 18:04:47
        await self.counters(0, DOWN)
        await self.late(DOWN - 100, reg(SLOW, 49.9, 499))
        await self.counter(UP)
        self.assertEqual(self.gaps(), [(DOWN, UP)])


# ─────────────────────────────────────────────────────────────────────────────
# Запоздалая пачка из прошлого не откатывает состояние
# ─────────────────────────────────────────────────────────────────────────────

class PastPacketTests(ReconnectTestCase):

    async def test_history_only_latest_state_and_tracker_kept(self):
        await self.pkt(0, reg(FUEL_P, 278.7, 2787))
        await self.pkt(9, reg(FUEL_P, 280.4, 2804))
        await self.counter(18)
        await self.late(5, reg(FUEL_P, 270.0, 2700))     # дошла после более свежих

        self.assertEqual(self.fake.t["latest"][FUEL_P], {"value": Decimal("280.4"), "raw": 2804})
        self.assertEqual(self.fake.t["latest_ts"][FUEL_P], at(9))
        self.assertEqual(
            self.series(FUEL_P),
            [(0, Decimal("278.7")), (5, Decimal("270.0")), (9, Decimal("280.4"))],
        )
        self.assertEqual(handlers._last_packet_ts[PANEL], at(18))
        self.assertEqual(handlers._panel_settled_ts[PANEL], at(9))
        self.assertEqual(handlers._last_write_ts[(SN, ET, PID, FUEL_P)], at(9))
        self.assertEqual(self.gaps(), [])

    async def test_no_enum_fault_transitions(self):
        await self.pkt(0, reg(ADDR_ENUM, 1, 1), reg(ADDR_FAULT, {"faults": []}, 0))
        await self.pkt(9, reg(ADDR_ENUM, 2, 2), reg(ADDR_FAULT, {"faults": [{"bit": 7}]}, 128))
        await self.counter(18)
        await self.late(5, reg(ADDR_ENUM, 1, 1), reg(ADDR_FAULT, {"faults": [{"bit": 2}]}, 4))

        enum = [(e["value"], e["start"], e["end"]) for e in self.fake.t["enum"]]
        self.assertEqual(enum, [(1, at(0), at(9)), (2, at(9), None)])
        fault = [(f["bit"], f["start"], f["end"]) for f in self.fake.t["fault"]]
        self.assertEqual(fault, [(7, at(9), None)])
        self.assertEqual(handlers._active_enum_states[(SN, ET, PID, ADDR_ENUM)], 2)
        self.assertEqual(handlers._active_fault_bits[(SN, ET, PID, ADDR_FAULT)], {7})
        # строки history на метке пачки — есть
        self.assertIn((5, Decimal(1)), self.series(ADDR_ENUM))
        self.assertIn((5, Decimal(4)), self.series(ADDR_FAULT))

    async def test_late_in_order_packet_history_only(self):
        # late и по порядку — всё равно только history (метке не верим)
        await self.pkt(0, reg(ADDR_ENUM, 1, 1))
        await self.late(9, reg(ADDR_ENUM, 2, 2))
        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [1])
        self.assertEqual(handlers._last_packet_ts[PANEL], at(0))
        self.assertIn((9, Decimal(2)), self.series(ADDR_ENUM))

    async def test_backward_packet_without_status_is_history_only(self):
        # Без пометки декодера, но метка раньше последней — трекер только растёт
        await self.counters(0, 18)
        await self.pkt(5, reg(ADDR_ENUM, 3, 3))
        self.assertEqual(handlers._last_packet_ts[PANEL], at(18))
        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [])
        self.assertEqual(self.series(ADDR_ENUM), [(5, Decimal(3))])

    async def test_offset_jump_is_history_only(self):
        await self.pkt(0, reg(ADDR_ENUM, 1, 1))
        await self.handle({**packet(reg(ADDR_ENUM, 2, 2), ts=at(9).isoformat()),
                           "ts_status": "offset_jump"})
        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [1])
        self.assertEqual(handlers._last_packet_ts[PANEL], at(0))


class LatestStateGuardTests(ReconnectTestCase):

    def setUp(self) -> None:
        self.real_upsert = db.upsert_latest_state_batch     # до подмены FakeDB
        super().setUp()

    async def test_upsert_sql_has_ts_guard(self):
        conn = mock.Mock()
        conn.executemany = mock.AsyncMock()
        await self.real_upsert(conn, [(SN, ET, PID, FUEL_P, at(0), Decimal(1), 1)])
        sql = " ".join(conn.executemany.await_args.args[0].split())
        self.assertIn(
            "WHERE latest_state.ts IS NULL OR EXCLUDED.ts >= latest_state.ts "
            "OR latest_state.ts > now() + interval '60 seconds'",
            sql,
        )

    async def test_future_row_from_old_version_overwritten(self):
        # Строка с меткой +2 ч осталась с версий до 2.9.0 (такие пакеты теперь
        # не пишутся): срез не замерзает до её наступления
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.fake.t["latest"][FUEL_P] = {"value": Decimal("278.7"), "raw": 2787}
        self.fake.t["latest_ts"][FUEL_P] = now + timedelta(hours=2)
        await self.handle(packet(reg(FUEL_P, 280.4, 2804), ts=now.isoformat()))
        self.assertEqual(self.fake.t["latest"][FUEL_P]["value"], Decimal("280.4"))


# ─────────────────────────────────────────────────────────────────────────────
# Первый кадр enum/масок после разрыва пишется и без изменения
# ─────────────────────────────────────────────────────────────────────────────

MASK_AT = UP + 171      # 24.10: маски пришли через 2 мин 51 с после остальных


class FirstFrameAfterGapTests(ReconnectTestCase):

    def enum(self, value: int) -> dict:
        return reg(ADDR_ENUM, value, value)

    def mask(self, *bits: int) -> dict:
        raw = sum(1 << b for b in bits)
        return reg(ADDR_FAULT, {"faults": [{"bit": b} for b in bits]}, raw)

    async def outage(self) -> None:
        await self.pkt(0, self.enum(1), self.mask(7))
        await self.counters(9, DOWN)

    async def test_unchanged_frame_after_gap_written_once(self):
        await self.outage()
        await self.pkt(UP, self.enum(1))                 # enum — в первом срезе
        await self.pkt(UP + 9, self.enum(1))
        await self.pkt(MASK_AT, self.mask(7))            # маски — позже
        await self.pkt(MASK_AT + 9, self.mask(7))

        self.assertEqual(self.series(ADDR_ENUM), [(0, Decimal(1)), (UP, Decimal(1))])
        self.assertEqual(self.series(ADDR_FAULT), [(0, Decimal(128)), (MASK_AT, Decimal(128))])
        # периоды не тронуты: бит 7 горит с 0, состояние 1 — с 0
        fault = [(f["bit"], f["start"], f["end"]) for f in self.fake.t["fault"]]
        self.assertEqual(fault, [(7, at(0), None)])
        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [1])
        self.assertEqual(len(self.fake.t["enum"]), 1)

    async def test_no_extra_rows_without_gap(self):
        await self.pkt(0, self.enum(1), self.mask(7))
        for sec in range(9, 200, 9):
            await self.pkt(sec, self.enum(1), self.mask(7))
        self.assertEqual(len(self.series(ADDR_ENUM)), 1)
        self.assertEqual(len(self.series(ADDR_FAULT)), 1)

    async def test_changed_frame_after_gap_written_once(self):
        await self.outage()
        await self.pkt(MASK_AT, self.mask(2))            # 40406: бит 2 вместо 7
        self.assertEqual(self.series(ADDR_FAULT), [(0, Decimal(128)), (MASK_AT, Decimal(4))])
        fault = [(f["bit"], f["start"], f["end"]) for f in self.fake.t["fault"]]
        self.assertEqual(fault, [(7, at(0), at(MASK_AT)), (2, at(MASK_AT), None)])

    async def test_after_restart(self):
        await self.outage()
        await self.counter(UP)
        await self.restart()                             # маски ещё не пришли
        await self.pkt(MASK_AT, self.mask(7))
        self.assertEqual(self.series(ADDR_FAULT), [(0, Decimal(128)), (MASK_AT, Decimal(128))])

    async def test_no_timer_heartbeat_for_masks(self):
        await self.outage()
        await self.pkt(UP, self.enum(1), self.mask(7))
        await self.counters(UP + 9, UP + 300)
        await self.flush()
        self.assertEqual([s for s, _ in self.series(ADDR_ENUM)], [0, UP])
        self.assertEqual([s for s, _ in self.series(ADDR_FAULT)], [0, UP])


# ─────────────────────────────────────────────────────────────────────────────
# Сценарий 24.10 целиком
# ─────────────────────────────────────────────────────────────────────────────

class Des3Outage2410Tests(ReconnectTestCase):

    async def test_outage(self):
        await self.before_outage()
        await self.late(STUCK, reg(SLOW, 49.9, 499))     # 11:48:09, дошла в 12:38:28
        await self.pkt(UP, reg(RPM, 0, 0))               # 12:38:58, машина стоит
        await self.counters(UP + 9, UP + 90)
        await self.flush()

        self.assertEqual(self.gaps(), [(DOWN, UP)])      # один разрыв, а не 615 + 616
        # рабочие значения со стоянки не повторены со свежей меткой
        self.assertEqual(self.series(FUEL_P), [(0, Decimal("278.7"))])
        self.assertEqual(
            self.series(SLOW), [(0, Decimal("49.9")), (STUCK, Decimal("49.9"))],
        )
        self.assertEqual(self.series(RPM), [(0, 0), (UP, 0), (UP + 81, 0)])


if __name__ == "__main__":
    unittest.main()
