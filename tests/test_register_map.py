# Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
# Программный комплекс «Честная Генерация»
# Модуль записи телеметрии в базу данных
# Автор: Саввиди Александр Анатольевич | ИНН 4725009270
#
# Данное программное обеспечение является конфиденциальным.
# Несанкционированное копирование, распространение или использование
# без письменного разрешения правообладателя запрещено.

"""Разбор ключей карты регистров cg/v1/maps/<device_type>.

Запуск из корня репозитория:
    python -m unittest discover -s tests -t .
"""

from __future__ import annotations

import logging
import unittest

from src import register_map

logging.getLogger("cg").addHandler(logging.NullHandler())

# Карта yts61 в том виде, как её публикует декодер (сервер, 09.10.2026)
YTS61_MAP = {
    "device_type": "yts61",
    "registers": {
        "36864":       {"name": "Sensor Address"},
        "input:30000": {"name": "Temperature", "unit": "°C"},
        "input:30002": {"name": "Water Activity", "unit": "aw"},
        "input:30004": {"name": "Moisture Content", "unit": "%"},
        "input:30006": {"name": "Dynamic Viscosity", "unit": "mPa·s"},
        "input:30008": {"name": "Density", "unit": "kg/m³"},
        "input:30010": {"name": "Dielectric Constant"},
        "input:30012": {"name": "Kinematic Viscosity", "unit": "mm²/s"},
    },
}


class RegisterMapKeysTests(unittest.TestCase):

    def setUp(self) -> None:
        saved = dict(register_map._maps)
        self.addCleanup(lambda: (register_map._maps.clear(), register_map._maps.update(saved)))
        register_map._maps.clear()

    def test_input_prefixed_keys_loaded(self):
        with self.assertNoLogs("cg.register_map", level="WARNING"):
            register_map.update("yts61", YTS61_MAP)

        self.assertEqual(
            sorted(register_map._maps["yts61"]),
            [30000, 30002, 30004, 30006, 30008, 30010, 30012, 36864],
        )
        # addr — как в decoded (input: 30000+смещение, без префикса)
        self.assertEqual(register_map.get_unit("yts61", 30012), "mm²/s")
        self.assertEqual(register_map.get_entry("yts61", 30000)["name"], "Temperature")

    def test_numeric_and_holding_prefixed_keys(self):
        register_map.update("pcc", {"registers": {
            "40070": {"unit": "seconds"},
            "holding:40010": {"unit": "enum"},
        }})
        self.assertEqual(register_map.get_unit("pcc", 40070), "seconds")
        self.assertEqual(register_map.get_unit("pcc", 40010), "enum")

    def test_bad_keys_skipped_with_warning(self):
        bad = ["abc", "coil:1", "input:", "input:x", "input:30012:1"]
        with self.assertLogs("cg.register_map", level="WARNING") as logs:
            register_map.update("x", {"registers": {
                **{k: {"unit": "?"} for k in bad},
                "40001": {"unit": "V"},
            }})

        self.assertEqual(list(register_map._maps["x"]), [40001])
        warnings = [line for line in logs.output if "bad addr key" in line]
        self.assertEqual(len(warnings), len(bad))

    def test_input_holding_collision_keeps_first(self):
        with self.assertLogs("cg.register_map", level="WARNING") as logs:
            register_map.update("x", {"registers": {
                "40010": {"unit": "enum"},
                "input:40010": {"unit": "V"},
            }})

        self.assertEqual(register_map.get_unit("x", 40010), "enum")
        self.assertIn("collides", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
