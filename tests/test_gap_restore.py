# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""Gap-детектор после рестарта сервиса.

Случай ДЭС №3, 13–14.10: связь пропала в 22:33:53, в 23:33 db-writer
перезапустился (деплой), в 07:06 дошёл застрявший блок с меткой 06:33:16,
с 07:07:20 связь вернулась. Ни 8-часовой обрыв, ни 34-минутная дыра не
попали в data_gaps: метка последнего пакета жила только в памяти.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

from datetime import timedelta

from src import handlers
from tests.test_heartbeat import PANEL, HeartbeatTestCase, at

STEP = 9            # цикл опроса, с
LAST = 450          # последний пакет перед рестартом (51 пакет с 0)


class GapRestoreTestCase(HeartbeatTestCase):

    async def run_until_restart(self) -> None:
        for sec in range(0, LAST + 1, STEP):
            await self.counter(sec)

    async def restart(self) -> None:
        """Новый процесс: память пуста, history и data_gaps — в БД."""
        last_write = dict(handlers._last_write_ts)   # то, что вернёт restore_write_timestamps
        for state in (
            handlers._last_packet_ts, handlers._avg_interval, handlers._panel_settled_ts,
            handlers._last_write_ts, handlers._heartbeat_ts,
            handlers._last_packet_late, handlers._panel_gap_end,
        ):
            state.clear()
        handlers._last_write_ts.update(last_write)
        await handlers.restore_gap_tracker()

    def gaps(self) -> list[tuple[float, float | None]]:
        return [
            ((g["start"] - at(0)).total_seconds(),
             None if g["end"] is None else (g["end"] - at(0)).total_seconds())
            for g in self.fake.t["gaps"]
        ]


class RestartTests(GapRestoreTestCase):

    async def test_silence_across_restart_is_gap(self):
        await self.run_until_restart()
        await self.restart()
        self.assertEqual(handlers._last_packet_ts[PANEL], at(LAST))
        self.assertEqual(handlers._avg_interval[PANEL], STEP)

        late = LAST + 8 * 3600                       # застрявший блок после 8 ч молчания
        await self.counter(late)
        await self.counter(late + 2044)              # связь вернулась через 34 мин
        await self.counter(late + 2044 + STEP)

        self.assertEqual(self.gaps(), [(LAST, late), (late, late + 2044)])

    async def test_normal_cadence_after_restart_no_gap(self):
        await self.run_until_restart()
        await self.restart()
        await self.counter(LAST + STEP)
        await self.counter(LAST + 2 * STEP)
        self.assertEqual(self.gaps(), [])

    async def test_short_deploy_downtime_no_gap(self):
        await self.run_until_restart()
        await self.restart()
        await self.counter(LAST + 40)                # 40 с пакетов потеряно на рестарте
        self.assertEqual(self.gaps(), [])

    async def test_panel_silent_beyond_window(self):
        await self.run_until_restart()
        self.fake.window_end = at(LAST) + timedelta(days=3)   # меток за сутки нет
        await self.restart()
        self.assertEqual(handlers._last_packet_ts[PANEL], at(LAST))   # из _last_write_ts
        self.assertNotIn(PANEL, handlers._avg_interval)

        back = LAST + 3 * 86400
        await self.counter(back)
        await self.counter(back + STEP)

        self.assertEqual(self.gaps(), [(LAST, back)])
        self.assertEqual(handlers._avg_interval[PANEL], STEP)   # задано нормальным интервалом


class FirstIntervalTests(GapRestoreTestCase):

    async def test_new_panel_first_interval_seeds_average(self):
        await self.counter(0)
        await self.counter(300)                      # редкие пакеты новой панели — не разрыв
        self.assertEqual(self.gaps(), [])
        self.assertEqual(handlers._avg_interval[PANEL], 300)

    async def test_restore_ignores_server_time_rows(self):
        await self.run_until_restart()
        # heartbeat до 2.7.2: метка сервера с дробной секундой позже последнего пакета
        self.fake.t["history"].append((40068, 0, 0, at(LAST + 100.137)))
        await self.restart()
        self.assertEqual(handlers._last_packet_ts[PANEL], at(LAST))
