# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""Watchdog: запоздалая пачка не возвращает панель в online.

Случай ДЭС №3, 24.10: панель offline с утра, в 12:38:28 дошла застрявшая
пачка с меткой 11:48:09 (ts_status=late) — panel_online в 12:38:49, хотя
живые пакеты пошли только с меткой 12:38:58.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import json
import unittest
from datetime import timedelta
from unittest import mock

from src import db, watchdog
from src import main as app
from src.config import AppConfig
from tests.test_decoded_write import ET, PID, SN, TOPIC, FakeDB, packet, reg

PANEL = (SN, ET, PID)


def payload(ts_status: str | None) -> bytes:
    """Пачка decoded так, как её публикует декодер (компактный JSON)."""
    data = packet(reg(40595, 49.9, 499))
    if ts_status is not None:
        data.update(received_at="2026-10-24T09:38:28.000+00:00", ts_status=ts_status)
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()


class WatchdogTestCase(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.fake = FakeDB()
        self.events: list[str] = []

        async def insert_event(conn, router_sn, event_type, **kwargs):
            self.events.append(event_type)

        for name, fn in (("pool", self.fake.pool), ("insert_event", insert_event)):
            p = mock.patch.object(db, name, fn)
            p.start()
            self.addCleanup(p.stop)

        for state in (
            app._last_seen, app._panel_last_seen, app._panel_last_live,
            watchdog._router_state, watchdog._panel_state,
        ):
            state.clear()
        self.cfg = AppConfig()

    def receive(self, ts_status: str | None = "ok") -> None:
        app._touch_last_seen(TOPIC, payload(ts_status))

    def wait(self, sec: float) -> None:
        """Прошло sec: всё увиденное уходит в прошлое."""
        for seen in (app._last_seen, app._panel_last_seen, app._panel_last_live):
            for key in seen:
                seen[key] -= timedelta(seconds=sec)

    async def check(self) -> None:
        await watchdog._check(
            self.cfg, app._last_seen, app._panel_last_seen, app._panel_last_live,
        )

    def panel_events(self) -> list[str]:
        return [e for e in self.events if e.startswith("panel_")]


class LatePacketTests(WatchdogTestCase):

    async def test_late_packet_does_not_bring_panel_online(self):
        self.receive()
        await self.check()
        self.wait(6 * 3600)                          # обрыв
        await self.check()
        self.assertEqual(self.panel_events(), ["panel_offline"])

        self.receive("late")                         # застрявшая пачка
        await self.check()
        self.assertEqual(self.panel_events(), ["panel_offline"])
        self.assertEqual(watchdog._panel_state[PANEL], "offline")

        self.wait(20)
        self.receive()                               # живые пакеты
        await self.check()
        self.assertEqual(self.panel_events(), ["panel_offline", "panel_online"])

    async def test_lone_late_packet_then_silence(self):
        self.receive()
        self.wait(3600)
        await self.check()
        self.receive("late")
        await self.check()
        self.wait(400)
        await self.check()
        self.assertEqual(self.panel_events(), ["panel_offline"])

    async def test_stale_panel_needs_live_packet(self):
        self.receive()
        self.wait(150)
        await self.check()
        self.assertEqual(watchdog._panel_state[PANEL], "stale")
        self.receive("late")
        await self.check()
        self.assertEqual(watchdog._panel_state[PANEL], "stale")
        self.receive()
        await self.check()
        self.assertEqual(self.panel_events(), ["panel_online"])

    async def test_late_packets_keep_online_panel_alive(self):
        # Роутер на связи досылает буфер или его часы отстают больше чем на
        # минуту (декодер метит такие записи late): панель не уходит в offline
        self.receive()
        for _ in range(10):
            self.wait(60)
            self.receive("late")
            await self.check()
        self.assertEqual(self.panel_events(), [])

    def test_touch_last_seen(self):
        self.receive("late")
        self.assertIn(PANEL, app._panel_last_seen)
        self.assertNotIn(PANEL, app._panel_last_live)
        self.assertIn(SN, app._last_seen)            # роутер — как раньше
        self.receive("ok")
        self.assertIn(PANEL, app._panel_last_live)

    def test_is_late(self):
        self.assertTrue(app._is_late(payload("late")))
        self.assertFalse(app._is_late(payload("ok")))
        self.assertFalse(app._is_late(payload(None)))          # декодер без clock_guard
        text = json.dumps({"registers": [{"text": "late"}], "ts_status": "ok"}).encode()
        self.assertFalse(app._is_late(text))
        self.assertFalse(app._is_late(b'["late"]'))
        self.assertFalse(app._is_late(b'\x1f\x8b"late"'))


if __name__ == "__main__":
    unittest.main()
