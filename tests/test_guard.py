# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""Проверки перед записью (задача cg-analytics 30.10): метки из будущего,
дубли (addr, ts), периоды без конца раньше начала, ts_status в history.

По БД до правки:
- 117 групп (addr, ts) в history, у 115 значения разные;
- 10 вывернутых периодов enum и 56 разрывов с концом в будущем (метки +2 ч);
- периоды enum/fault, открытые и закрытые на метке поздней записи.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import random
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

from src import db, handlers
from src import main as app
from src.health import HealthState, _payload
from tests.test_decoded_write import ADDR_ENUM, ADDR_FAULT, ET, PID, SN, packet, reg
from tests.test_heartbeat import FUEL_P, PANEL, HeartbeatTestCase, at


def stamped(sec: float, *regs: dict, ts_status: str | None = None) -> dict:
    data = packet(*regs, ts=at(sec).isoformat())
    if ts_status is not None:
        data["ts_status"] = ts_status
    return data


class FutureTests(HeartbeatTestCase):

    async def test_future_packet_not_written(self):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        await self.counter(0)
        with self.assertLogs("cg.handler", level="WARNING") as logs:
            for _ in range(2):
                await self.handle(packet(reg(ADDR_ENUM, 1, 1), reg(FUEL_P, 278.7, 2787),
                                         ts=(now + timedelta(hours=2)).isoformat()))

        self.assertEqual(self.fake.history_for(ADDR_ENUM), [])
        self.assertEqual(self.fake.history_for(FUEL_P), [])
        self.assertNotIn(FUEL_P, self.fake.t["latest"])
        self.assertEqual(self.fake.t["enum"], [])
        self.assertEqual(self.fake.t["gaps"], [])
        self.assertEqual(handlers._last_packet_ts[PANEL], at(0))
        self.assertEqual(handlers.guard_stats["future_dropped"], 2)
        future = [line for line in logs.output if "Future timestamp" in line]
        self.assertEqual(len(future), 1)                 # раз в час на панель

    async def test_slightly_ahead_written(self):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        await self.handle(packet(reg(FUEL_P, 278.7, 2787),
                                 ts=(now + timedelta(seconds=30)).isoformat()))
        self.assertEqual(len(self.fake.history_for(FUEL_P)), 1)
        self.assertEqual(handlers.guard_stats["future_dropped"], 0)


class PeriodTests(HeartbeatTestCase):

    async def test_late_record_does_not_close_stop(self):
        # 30.09: стоп закрыт на 29.09 18:02:53 поздней записью «Работа»
        await self.pkt(0, reg(ADDR_ENUM, 1, 1))          # работа
        await self.pkt(90, reg(ADDR_ENUM, 0, 0))         # стоп
        await self.handle(stamped(50, reg(ADDR_ENUM, 1, 1)))   # без пометки, но раньше
        enum = [(e["value"], e["start"], e["end"]) for e in self.fake.t["enum"]]
        self.assertEqual(enum, [(1, at(0), at(90)), (0, at(90), None)])

    async def test_no_inverted_periods_in_shuffled_stream(self):
        rnd = random.Random(30)
        frames = [(sec, rnd.choice([0, 1, 2]), rnd.choice([0, 4, 128]))
                  for sec in range(0, 900, 9)]
        rnd.shuffle(frames)
        for sec, state, mask in frames:
            bits = [b for b in (2, 7) if mask & (1 << b)]
            await self.handle(stamped(
                sec, reg(ADDR_ENUM, state, state),
                reg(ADDR_FAULT, {"faults": [{"bit": b} for b in bits]}, mask),
                ts_status=rnd.choice([None, "ok", "late"]),
            ))
        for row in self.fake.t["enum"] + self.fake.t["fault"]:
            if row["end"] is not None:
                self.assertGreaterEqual(row["end"], row["start"], row)
        self.assertLessEqual(len(self.fake.open_enum(ADDR_ENUM)), 1)


class DuplicateTests(HeartbeatTestCase):

    async def test_first_arrival_wins_conflict_counted(self):
        await self.pkt(0, reg(FUEL_P, 278.7, 2787))
        with self.assertLogs("cg.handler", level="WARNING") as logs:
            await self.pkt(0, reg(FUEL_P, 280.4, 2804))      # тот же ts, другое значение
            await self.pkt(0, reg(FUEL_P, 281.0, 2810))
        self.assertEqual(self.fake.series(FUEL_P), [(at(0), Decimal("278.7"))])
        self.assertEqual(handlers.guard_stats["history_conflicts"], 2)
        conflicts = [line for line in logs.output if "History conflict" in line]
        self.assertEqual(len(conflicts), 1)              # раз в час на панель

    async def test_same_value_is_not_a_conflict(self):
        await self.pkt(0, reg(ADDR_ENUM, 1, 1))
        handlers._last_write_ts.clear()                  # как после рестарта: запись повторится
        handlers._last_packet_ts.clear()
        await self.pkt(0, reg(ADDR_ENUM, 1, 1))
        self.assertEqual(len(self.fake.history_for(ADDR_ENUM)), 1)
        self.assertEqual(handlers.guard_stats["history_conflicts"], 0)

    async def test_before_migration_everything_inserted(self):
        self.fake.unique_history = False                 # индекса ещё нет
        await self.pkt(0, reg(FUEL_P, 278.7, 2787))
        await self.pkt(0, reg(FUEL_P, 280.4, 2804))
        self.assertEqual(len(self.fake.history_for(FUEL_P)), 2)
        self.assertEqual(handlers.guard_stats["history_conflicts"], 0)


class TsStatusTests(HeartbeatTestCase):

    async def test_ts_status_stored_and_counted(self):
        await self.handle(stamped(0, reg(FUEL_P, 278.7, 2787)))
        await self.handle(stamped(9, reg(FUEL_P, 280.4, 2804), ts_status="clock_fixed"))
        await self.handle(stamped(5, reg(FUEL_P, 279.0, 2790), ts_status="late"))
        await self.handle(stamped(18, reg(FUEL_P, 290.0, 2900), ts_status="ok"))
        status = {h[3]: h[4] for h in self.fake.history_for(FUEL_P)}
        self.assertEqual(status, {at(0): None, at(9): "clock_fixed", at(5): "late", at(18): "ok"})
        self.assertEqual(handlers.guard_stats["ts_status"],
                         {"none": 1, "clock_fixed": 1, "late": 1, "ok": 1})
        self.assertEqual(handlers.guard_stats["history_only"], 1)

    async def test_heartbeat_rows_have_no_status(self):
        await self.handle(stamped(0, reg(FUEL_P, 278.7, 2787), ts_status="ok"))
        for sec in range(9, 73, 9):
            await self.pkt(sec, reg(40290, 1000 + sec, int(1000 + sec)))
        await self.flush()
        rows = [h for h in self.fake.history_for(FUEL_P) if h[3] != at(0)]
        self.assertEqual([(h[3], h[4]) for h in rows], [(at(63), None)])

    def test_health_payload(self):
        handlers.guard_stats["future_dropped"] = 3
        handlers.guard_stats["ts_status"]["late"] = 5
        guard = _payload(HealthState())["data_guard"]
        self.assertEqual(guard["future_dropped"], 3)
        self.assertEqual(guard["ts_status"], {"late": 5})

    def test_panel_online_ignores_offset_jump(self):
        data = b'{"timestamp":"2026-10-30T12:00:00+0300","registers":[],"ts_status":"offset_jump"}'
        self.assertTrue(app._is_late(data))


class HistorySqlTests(unittest.IsolatedAsyncioTestCase):
    """SQL функций src.db — на подставном соединении (PostgreSQL в тестах нет)."""

    ROWS = [
        (SN, ET, PID, FUEL_P, at(0), Decimal("278.7"), 2787, "ok"),
        (SN, ET, PID, ADDR_ENUM, at(0), Decimal(1), 1, "ok"),
    ]

    def conn(self, returned: list[dict]) -> mock.Mock:
        conn = mock.Mock()
        conn.fetch = mock.AsyncMock(return_value=returned)
        return conn

    def tearDown(self) -> None:
        db._history_has_ts_status = True

    async def test_insert_on_conflict_returns_rejected(self):
        # PostgreSQL отдаёт ts в UTC — сравнение по моменту, не по поясу
        utc = at(0).astimezone(timezone.utc)
        conn = self.conn([{"addr": ADDR_ENUM, "ts": utc}])
        rejected = await db.insert_history_batch(conn, self.ROWS)
        self.assertEqual(rejected, [self.ROWS[0]])
        sql, *args = conn.fetch.await_args.args
        sql = " ".join(sql.split())
        self.assertIn("ts_status) SELECT * FROM unnest(", sql)
        self.assertIn("ON CONFLICT DO NOTHING RETURNING addr, ts", sql)
        self.assertEqual(len(args), 8)
        self.assertEqual(args[7], ["ok", "ok"])

    async def test_insert_without_ts_status_column(self):
        db._history_has_ts_status = False
        conn = self.conn([{"addr": r[3], "ts": r[4]} for r in self.ROWS])
        self.assertEqual(await db.insert_history_batch(conn, self.ROWS), [])
        sql, *args = conn.fetch.await_args.args
        self.assertNotIn("ts_status", sql)
        self.assertEqual(len(args), 7)

    async def test_detect_column(self):
        conn = mock.Mock()
        conn.fetchval = mock.AsyncMock(return_value=False)
        self.assertFalse(await db.detect_history_ts_status(conn))
        self.assertFalse(db._history_has_ts_status)


if __name__ == "__main__":
    unittest.main()
