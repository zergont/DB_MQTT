# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""Запись пакетов decoded: 32-битные «нет данных», raw вне int4,
состояние в памяти после commit, ретраи воркера, учёт незаписанных пакетов.

Слой src.db подменяется FakeDB: таблицы в памяти, транзакция откатывается
при исключении, raw вне int4 даёт asyncpg.DataError — как в PostgreSQL.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import asyncio
import copy
import logging
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import asyncpg

from src import db, handlers, register_map
from src import main as app
from src.config import AppConfig
from src.health import FAILED_RECENT_MAX, HealthState, _failed_payload
from src.history_policy import resolve_params, should_write

# Без настроенного логирования WARNING ушли бы в stderr через lastResort
logging.getLogger("cg").addHandler(logging.NullHandler())

SN, ET, PID = "1126246373", "pcc", 1
TOPIC = f"cg/v1/decoded/SN/{SN}/{ET}/{PID}"

ADDR_HOURS = 40070      # u32, Engine Running Time
ADDR_PRESSURE = 40100   # u16 аналог
ADDR_VOLTAGE = 40101    # u16 аналог
ADDR_ENUM = 40010
ADDR_FAULT = 40400

INT4_MAX = 2**31 - 1


def reg(addr: int, value, raw, **extra) -> dict:
    return {"addr": addr, "value": value, "raw": raw, **extra}


def packet(*regs: dict, ts: str = "2026-10-09T12:03:57+00:00") -> dict:
    return {"timestamp": ts, "registers": list(regs)}


def na_reg(addr: int, raw: int) -> dict:
    """Регистр «нет данных» в том виде, как его публикует декодер."""
    return reg(addr, None, raw, na=True)


def normal_regs() -> list[dict]:
    return [reg(ADDR_PRESSURE, 1500, 1500), reg(ADDR_VOLTAGE, 230.5, 2305)]


class FakeDB:
    """Таблицы db-writer в памяти + подмена функций src.db."""

    FUNCS = (
        "pool", "upsert_object", "upsert_equipment",
        "insert_data_gap", "close_data_gap", "extend_data_gap",
        "get_latest_state_rows_many", "upsert_latest_state_batch",
        "insert_history_batch", "insert_event_batch",
        "open_enum_state_batch", "close_enum_states_batch",
        "open_fault_batch", "close_faults_batch",
        "get_open_gaps", "get_recent_packet_ts", "get_last_gap_ends",
    )

    def __init__(self) -> None:
        self.t: dict = {
            "latest": {},     # addr → {"value", "raw"}
            "latest_ts": {},  # addr → ts строки latest_state
            "history": [],    # (addr, value, raw, ts)
            "enum": [],       # {"addr", "value", "start", "end"}
            "fault": [],      # {"addr", "bit", "start", "end"}
            "gaps": [],
            "events": [],
        }
        self.fail_once: set[str] = set()   # имена функций, которые упадут один раз
        self.window_end: datetime | None = None   # «сейчас» для окна get_recent_packet_ts
        self.calls: list[str] = []

    # ── транзакции ──────────────────────────────────────────────────────────

    @asynccontextmanager
    async def _transaction(self):
        snapshot = copy.deepcopy(self.t)
        try:
            yield
        except BaseException:
            self.t = snapshot
            raise

    @asynccontextmanager
    async def _acquire(self):
        conn = mock.Mock()
        conn.transaction = self._transaction
        yield conn

    def pool(self):
        p = mock.Mock()
        p.acquire = self._acquire
        return p

    # ── helpers ─────────────────────────────────────────────────────────────

    def _enter(self, name: str) -> None:
        self.calls.append(name)
        if name in self.fail_once:
            self.fail_once.discard(name)
            raise ConnectionResetError(f"simulated DB failure in {name}")

    @staticmethod
    def _check_int4(rows: list[tuple]) -> None:
        # Как asyncpg при кодировании $7 (raw) в столбец INT
        for i, r in enumerate(rows):
            raw = r[6]
            if raw is not None and not (-2**31 <= raw <= INT4_MAX):
                raise asyncpg.DataError(
                    f"invalid input for query argument $7 in element #{i} of "
                    f"executemany() sequence: {raw} (value out of int32 range)"
                )

    # ── подменяемые функции src.db ──────────────────────────────────────────

    async def upsert_object(self, conn, router_sn):
        self._enter("upsert_object")

    async def upsert_equipment(self, conn, router_sn, equip_type, panel_id):
        self._enter("upsert_equipment")

    async def insert_data_gap(self, conn, router_sn, equip_type, panel_id, gap_start):
        self._enter("insert_data_gap")
        self.t["gaps"].append({"start": gap_start, "end": None})
        return len(self.t["gaps"])

    async def close_data_gap(self, conn, router_sn, equip_type, panel_id, gap_end):
        self._enter("close_data_gap")
        n = 0
        for g in self.t["gaps"]:
            if g["end"] is None:
                g["end"] = gap_end
                n += 1
        return n

    async def extend_data_gap(self, conn, router_sn, equip_type, panel_id, old_end, new_end):
        self._enter("extend_data_gap")
        n = 0
        for g in self.t["gaps"]:
            if g["end"] == old_end:
                g["end"] = new_end
                n += 1
        return n

    async def get_open_gaps(self, conn):
        self._enter("get_open_gaps")
        return [g for g in self.t["gaps"] if g["end"] is None]

    async def get_last_gap_ends(self, conn):
        self._enter("get_last_gap_ends")
        ends = [g["end"] for g in self.t["gaps"] if g["end"] is not None]
        if not ends:
            return []
        return [{"router_sn": SN, "equip_type": ET, "panel_id": PID, "gap_end": max(ends)}]

    async def get_recent_packet_ts(self, conn, days, per_panel):
        self._enter("get_recent_packet_ts")
        stamps = sorted({h[3] for h in self.t["history"] if h[3].microsecond == 0})
        if not stamps:
            return []
        end = self.window_end or stamps[-1]
        stamps = [t for t in stamps if t > end - timedelta(days=days)][-per_panel:]
        return [{"router_sn": SN, "equip_type": ET, "panel_id": PID, "ts": t} for t in stamps]

    async def get_latest_state_rows_many(self, conn, router_sn, equip_type, panel_id, addrs):
        self._enter("get_latest_state_rows_many")
        return {
            a: {**self.t["latest"][a], "ts": self.t["latest_ts"][a]}
            for a in addrs if a in self.t["latest"]
        }

    async def upsert_latest_state_batch(self, conn, rows):
        self._enter("upsert_latest_state_batch")
        self._check_int4(rows)
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        for r in rows:
            # Как WHERE в ON CONFLICT: к более старой метке строка не откатывается,
            # если её метка не убежала вперёд часов сервера
            cur = self.t["latest_ts"].get(r[3])
            if cur is not None and r[4] < cur and cur <= future:
                continue
            self.t["latest"][r[3]] = {"value": r[5], "raw": r[6]}
            self.t["latest_ts"][r[3]] = r[4]

    async def insert_history_batch(self, conn, rows):
        self._enter("insert_history_batch")
        self._check_int4(rows)
        self.t["history"].extend((r[3], r[5], r[6], r[4]) for r in rows)

    async def insert_event_batch(self, conn, rows):
        self._enter("insert_event_batch")
        self.t["events"].extend(rows)

    async def open_enum_state_batch(self, conn, rows):
        self._enter("open_enum_state_batch")
        for r in rows:
            self.t["enum"].append({"addr": r[3], "value": r[4], "start": r[5], "end": None})

    async def close_enum_states_batch(self, conn, rows):
        self._enter("close_enum_states_batch")
        for r in rows:
            for e in self.t["enum"]:
                if e["addr"] == r[3] and e["end"] is None:
                    e["end"] = r[4]

    async def open_fault_batch(self, conn, rows):
        self._enter("open_fault_batch")
        for r in rows:
            self.t["fault"].append({"addr": r[3], "bit": r[4], "start": r[5], "end": None})

    async def close_faults_batch(self, conn, rows):
        self._enter("close_faults_batch")
        for r in rows:
            for f in self.t["fault"]:
                if f["addr"] == r[3] and f["bit"] == r[4] and f["end"] is None:
                    f["end"] = r[5]

    # ── выборки для проверок ────────────────────────────────────────────────

    def history_for(self, addr: int) -> list[tuple]:
        return [h for h in self.t["history"] if h[0] == addr]

    def series(self, addr: int) -> list[tuple[datetime, Decimal | None]]:
        """(ts, value) регистра в порядке ts — как его видит график."""
        return sorted((h[3], h[1]) for h in self.history_for(addr))

    def open_enum(self, addr: int) -> list[int]:
        return [e["value"] for e in self.t["enum"] if e["addr"] == addr and e["end"] is None]

    def open_fault_bits(self, addr: int) -> set[int]:
        return {f["bit"] for f in self.t["fault"] if f["addr"] == addr and f["end"] is None}


class DecodedTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.fake = FakeDB()
        for name in FakeDB.FUNCS:
            p = mock.patch.object(db, name, getattr(self.fake, name))
            p.start()
            self.addCleanup(p.stop)

        for state in (
            handlers._last_write_ts, handlers._last_packet_ts, handlers._avg_interval,
            handlers._active_fault_bits, handlers._active_enum_states,
            handlers._raw_overflow_warned, handlers._heartbeat_ts, handlers._panel_settled_ts,
            handlers._last_packet_late, handlers._panel_gap_end,
        ):
            state.clear()

        saved_maps = dict(register_map._maps)
        self.addCleanup(lambda: (register_map._maps.clear(), register_map._maps.update(saved_maps)))
        register_map._maps[ET] = {
            ADDR_ENUM:  {"unit": "enum"},
            ADDR_FAULT: {"unit": "fault_bitmap"},
        }

        self.cfg = AppConfig()
        self.cfg.history_policy.defaults.min_interval_sec = 0

    async def handle(self, data: dict) -> None:
        await handlers._handle_decoded(SN, ET, PID, data, self.cfg)

    def assert_normal_regs_written(self) -> None:
        latest = self.fake.t["latest"]
        self.assertEqual(latest[ADDR_PRESSURE], {"value": Decimal("1500"), "raw": 1500})
        self.assertEqual(latest[ADDR_VOLTAGE], {"value": Decimal("230.5"), "raw": 2305})
        self.assertTrue(self.fake.history_for(ADDR_PRESSURE))
        self.assertTrue(self.fake.history_for(ADDR_VOLTAGE))


# ─────────────────────────────────────────────────────────────────────────────
# «Нет данных» у u32 и raw вне int4
# ─────────────────────────────────────────────────────────────────────────────

class NaAndOverflowTests(DecodedTestCase):

    async def check_na_u32(self, raw: int) -> None:
        await self.handle(packet(na_reg(ADDR_HOURS, raw), *normal_regs()))

        self.assertEqual(self.fake.t["latest"][ADDR_HOURS], {"value": None, "raw": None})
        self.assertEqual([h[:3] for h in self.fake.history_for(ADDR_HOURS)], [(ADDR_HOURS, None, None)])
        self.assert_normal_regs_written()

    async def test_na_u32_fffffffc_packet_written(self):
        await self.check_na_u32(0xFFFFFFFC)

    async def test_na_u32_ffffffff_packet_written(self):
        await self.check_na_u32(0xFFFFFFFF)

    async def test_na_u16_raw_also_null(self):
        await self.handle(packet(na_reg(ADDR_PRESSURE, 0xFFFC)))
        self.assertEqual(self.fake.t["latest"][ADDR_PRESSURE], {"value": None, "raw": None})

    async def test_raw_out_of_int4_written_as_null_with_warning(self):
        raw = 3_168_000_000            # 88 000 моточасов в 0,1 с
        value = 316_800_000.0
        with self.assertLogs("cg.handler", level="WARNING") as logs:
            await self.handle(packet(reg(ADDR_HOURS, value, raw), *normal_regs()))

        self.assertEqual(
            self.fake.t["latest"][ADDR_HOURS], {"value": Decimal("316800000.0"), "raw": None},
        )
        self.assert_normal_regs_written()
        warning = "\n".join(logs.output)
        self.assertIn(f"{SN}/{ET}/{PID} addr={ADDR_HOURS}", warning)
        self.assertIn(f"raw={raw}", warning)

    async def test_out_of_int4_warning_throttled(self):
        with self.assertLogs("cg.handler", level="WARNING") as logs:
            for i in range(3):
                await self.handle(packet(reg(ADDR_HOURS, 316_800_000.0 + i, 3_168_000_000 + 10 * i)))
        overflow = [line for line in logs.output if "out of int4" in line]
        self.assertEqual(len(overflow), 1)

    async def test_int4_boundaries(self):
        await self.handle(packet(reg(ADDR_HOURS, 1.0, INT4_MAX), reg(ADDR_PRESSURE, -1.0, -2**31)))
        self.assertEqual(self.fake.t["latest"][ADDR_HOURS]["raw"], INT4_MAX)
        self.assertEqual(self.fake.t["latest"][ADDR_PRESSURE]["raw"], -2**31)

    async def test_history_change_detected_by_value_when_raw_null(self):
        await self.handle(packet(reg(ADDR_HOURS, 316_800_000.0, 3_168_000_000),
                                 ts="2026-10-09T12:00:00+00:00"))
        await self.handle(packet(reg(ADDR_HOURS, 316_800_000.0, 3_168_000_000),   # без изменений
                                 ts="2026-10-09T12:00:09+00:00"))
        await self.handle(packet(reg(ADDR_HOURS, 316_800_036.0, 3_168_000_360),   # +36 с
                                 ts="2026-10-09T12:00:18+00:00"))

        self.assertEqual(
            [h[1] for h in self.fake.history_for(ADDR_HOURS)],
            [Decimal("316800000.0"), Decimal("316800036.0")],
        )


class ShouldWriteTests(unittest.TestCase):

    def setUp(self) -> None:
        cfg = AppConfig()
        self.params = resolve_params(cfg, ET, ADDR_HOURS, None, {})
        self.now = datetime(2026, 10, 9, tzinfo=timezone.utc)
        self.last = self.now - timedelta(seconds=10)    # heartbeat (900 с) не наступил

    def decide(self, new_value, new_raw, prev_value, prev_raw):
        return should_write(
            self.params,
            new_value=new_value, new_raw=new_raw,
            prev_value=prev_value, prev_raw=prev_raw,
            last_write_ts=self.last, now=self.now,
        )

    def test_raw_null_both_sides_compares_value(self):
        self.params.tolerance = None
        d = self.decide(Decimal("316800036"), None, Decimal("316800000"), None)
        self.assertEqual((d.write, d.write_reason), (True, "change"))
        d = self.decide(Decimal("316800000"), None, Decimal("316800000.0"), None)
        self.assertFalse(d.write)

    def test_na_to_na_is_not_a_change(self):
        self.assertFalse(self.decide(None, None, None, None).write)

    def test_value_to_na_is_a_change(self):
        self.assertTrue(self.decide(None, None, Decimal("15"), 150).write)


# ─────────────────────────────────────────────────────────────────────────────
# Состояние в памяти обновляется только после commit
# ─────────────────────────────────────────────────────────────────────────────

class StateAfterCommitTests(DecodedTestCase):

    async def test_enum_transition_not_lost_after_failed_attempt(self):
        key = (SN, ET, PID, ADDR_ENUM)
        await self.handle(packet(reg(ADDR_ENUM, 1, 1)))
        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [1])
        packet_ts_before = dict(handlers._last_packet_ts)

        self.fake.fail_once.add("insert_history_batch")
        data = packet(reg(ADDR_ENUM, 2, 2), ts="2026-10-09T12:04:07+00:00")
        with self.assertRaises(ConnectionResetError):
            await self.handle(data)

        # Откат: ни БД, ни память не изменились
        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [1])
        self.assertEqual(handlers._active_enum_states[key], 1)
        self.assertEqual(handlers._last_packet_ts, packet_ts_before)

        await self.handle(data)    # повтор воркера

        self.assertEqual(self.fake.open_enum(ADDR_ENUM), [2])
        self.assertEqual(len(self.fake.t["enum"]), 2)    # период 1 закрыт, период 2 открыт
        self.assertEqual(handlers._active_enum_states[key], 2)

    async def test_fault_bit_not_lost_after_failed_attempt(self):
        key = (SN, ET, PID, ADDR_FAULT)
        data = packet(reg(ADDR_FAULT, {"faults": [{"bit": 3}]}, 8))

        self.fake.fail_once.add("open_fault_batch")
        with self.assertRaises(ConnectionResetError):
            await self.handle(data)
        self.assertNotIn(key, handlers._active_fault_bits)
        self.assertEqual(self.fake.open_fault_bits(ADDR_FAULT), set())

        await self.handle(data)

        self.assertEqual(self.fake.open_fault_bits(ADDR_FAULT), {3})
        self.assertEqual(handlers._active_fault_bits[key], {3})

    async def test_write_ts_not_updated_on_failure(self):
        self.fake.fail_once.add("insert_history_batch")
        with self.assertRaises(ConnectionResetError):
            await self.handle(packet(reg(ADDR_PRESSURE, 1500, 1500)))
        self.assertEqual(handlers._last_write_ts, {})

        await self.handle(packet(reg(ADDR_PRESSURE, 1500, 1500)))
        self.assertIn((SN, ET, PID, ADDR_PRESSURE), handlers._last_write_ts)


# ─────────────────────────────────────────────────────────────────────────────
# Ошибка данных: адрес регистра в сообщении, без ретраев
# ─────────────────────────────────────────────────────────────────────────────

class DataErrorTests(unittest.IsolatedAsyncioTestCase):

    ROWS = [
        (SN, ET, PID, ADDR_PRESSURE, None, Decimal(1), 1),
        (SN, ET, PID, ADDR_HOURS, None, Decimal(2), 2),
    ]

    async def test_error_names_failed_register(self):
        async def write(conn, rows):
            raise asyncpg.DataError(
                "invalid input for query argument $7 in element #1 of "
                "executemany() sequence: 4294967292 (value out of int32 range)"
            )

        with self.assertRaises(handlers.PacketDataError) as cm:
            await handlers._write_batch("latest_state", write, None, self.ROWS, (SN, ET, PID))
        msg = str(cm.exception)
        self.assertIn(f"latest_state {SN}/{ET}/{PID} addr={ADDR_HOURS}", msg)
        self.assertIn("4294967292", msg)

    async def test_error_without_element_lists_batch_addrs(self):
        async def write(conn, rows):
            raise asyncpg.DataError("numeric field overflow")

        with self.assertRaises(handlers.PacketDataError) as cm:
            await handlers._write_batch("history", write, None, self.ROWS, (SN, ET, PID))
        self.assertIn(f"addrs={[ADDR_HOURS, ADDR_PRESSURE]}", str(cm.exception))


class WorkerRetryTests(unittest.IsolatedAsyncioTestCase):

    async def run_worker(self, side_effect) -> tuple[mock.AsyncMock, HealthState]:
        cfg = AppConfig()
        cfg.ingest.worker_retry_delay_sec = 0
        q_tel: asyncio.Queue = asyncio.Queue()
        q_dec: asyncio.Queue = asyncio.Queue()
        q_dec.put_nowait(app._IngestItem(TOPIC, b"{}", datetime.now(timezone.utc)))
        health = HealthState()

        dispatch = mock.AsyncMock(side_effect=side_effect)
        with mock.patch.object(app, "dispatch", dispatch):
            task = asyncio.create_task(app._worker_loop(1, cfg, q_tel, q_dec, health))
            try:
                await asyncio.wait_for(q_dec.join(), timeout=2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        return dispatch, health

    async def test_data_error_is_not_retried(self):
        for exc in (
            handlers.PacketDataError("latest_state x/pcc/1 addr=40070: out of int32 range"),
            asyncpg.DataError("invalid input"),
        ):
            with self.subTest(exc=type(exc).__name__):
                with self.assertLogs("cg.main", level="ERROR") as logs:
                    dispatch, health = await self.run_worker(exc)
                self.assertEqual(dispatch.await_count, 1)
                self.assertIn("no retry", "\n".join(logs.output))

                self.assertEqual(health.failed_total, 1)
                failed = health.failed_recent[-1]
                self.assertEqual((failed.topic, failed.attempts), (TOPIC, 1))
                self.assertIn(type(exc).__name__, failed.reason)

    async def test_transient_error_is_retried(self):
        dispatch, health = await self.run_worker([ConnectionResetError("db restart"), None])
        self.assertEqual(dispatch.await_count, 2)
        self.assertEqual(health.failed_total, 0)

    async def test_failure_after_retries_is_recorded(self):
        with self.assertLogs("cg.main", level="ERROR"):
            dispatch, health = await self.run_worker(ConnectionResetError("db down"))
        self.assertEqual(dispatch.await_count, 4)       # 1 попытка + 3 ретрая
        self.assertEqual(health.failed_total, 1)
        failed = health.failed_recent[-1]
        self.assertEqual(failed.attempts, 4)
        self.assertEqual(failed.reason, "ConnectionResetError: db down")


# ─────────────────────────────────────────────────────────────────────────────
# Незаписанные пакеты в health (/health/failed)
# ─────────────────────────────────────────────────────────────────────────────

class FailedPacketsHealthTests(unittest.TestCase):

    def test_payload_newest_first(self):
        health = HealthState()
        health.mark_failed("t/1", 4, ConnectionResetError("first"))
        health.mark_failed("t/2", 1, handlers.PacketDataError("second"))

        payload = _failed_payload(health)
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["since"], health.started_at.isoformat())
        self.assertEqual([f["topic"] for f in payload["recent"]], ["t/2", "t/1"])
        self.assertEqual(payload["recent"][0]["reason"], "PacketDataError: second")
        self.assertEqual(payload["recent"][1]["attempts"], 4)

    def test_recent_is_bounded_total_is_not(self):
        health = HealthState()
        for i in range(FAILED_RECENT_MAX + 7):
            health.mark_failed(f"t/{i}", 1, ValueError(str(i)))

        payload = _failed_payload(health)
        self.assertEqual(payload["total"], FAILED_RECENT_MAX + 7)
        self.assertEqual(len(payload["recent"]), FAILED_RECENT_MAX)
        self.assertEqual(payload["recent"][0]["topic"], f"t/{FAILED_RECENT_MAX + 6}")

    def test_long_reason_truncated(self):
        health = HealthState()
        health.mark_failed("t", 1, ValueError("x" * 2000))
        reason = health.failed_recent[-1].reason
        self.assertLessEqual(len(reason), 501)
        self.assertTrue(reason.endswith("…"))


if __name__ == "__main__":
    unittest.main()
