#!/usr/bin/env python3
"""
Тестовый скрипт: публикует GPS и decoded сообщения в MQTT для проверки DB-Writer.

Требует:  pip install paho-mqtt

Использование:
    python scripts/test_publish.py --host localhost --port 1883

Скрипт отправляет:
  1) Нормальную GPS точку
  2) Телепорт-GPS (должна быть rejected)
  3) Decoded с 3 регистрами (один normal, один NA, один unknown)
"""

import argparse
import json
import time

import paho.mqtt.client as mqtt


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--user", default="")
    p.add_argument("--password", default="")
    p.add_argument("--sn", default="6003790403", help="router_sn для теста")
    args = p.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="cg-test-pub")
    if args.user:
        client.username_pw_set(args.user, args.password)
    client.connect(args.host, args.port, 60)

    sn = args.sn
    topic_gps = f"cg/v1/telemetry/SN/{sn}"
    topic_dec = f"cg/v1/decoded/SN/{sn}/pcc/1"

    # --- 1) GPS нормальная точка ---
    gps1 = {
        "GPS": {
            "latitude": 59.851624,
            "longitude": 30.479838,
            "satellites": 8,
            "fix_status": 1,
            "timestamp": int(time.time()),
            "date_iso_8601": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
    }
    client.publish(topic_gps, json.dumps(gps1))
    print(f"[1/4] GPS normal  → {topic_gps}")
    time.sleep(1)

    # --- 2) GPS телепорт (далеко) ---
    gps2 = {
        "GPS": {
            "latitude": 55.751244,
            "longitude": 37.618423,
            "satellites": 10,
            "fix_status": 1,
            "timestamp": int(time.time()),
            "date_iso_8601": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
    }
    client.publish(topic_gps, json.dumps(gps2))
    print(f"[2/4] GPS teleport → {topic_gps}  (expect rejected)")
    time.sleep(1)

    # --- 3) Decoded — нормальные регистры + NA + unknown ---
    decoded = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "router_sn": sn,
        "bserver_id": 1,
        "registers": [
            {
                "addr": 40034,
                "name": "GensetTotal kW",
                "value": 150.5,
                "text": "150.5",
                "unit": "kW",
                "raw": 1505,
                "reason": None,
            },
            {
                "addr": 40062,
                "name": "OilPressure",
                "value": None,
                "text": None,
                "unit": "kPa",
                "raw": None,
                "reason": "Значение NA",
            },
            {
                "addr": 49999,
                "name": None,
                "value": 42,
                "text": "42",
                "unit": None,
                "raw": 42,
                "reason": "Неизвестный регистр",
            },
        ],
    }
    client.publish(topic_dec, json.dumps(decoded))
    print(f"[3/4] Decoded 3 regs → {topic_dec}")
    time.sleep(1)

    # --- 4) Decoded — повторная отправка того же (проверка deadband/min_interval) ---
    decoded["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    client.publish(topic_dec, json.dumps(decoded))
    print(f"[4/4] Decoded repeat → {topic_dec}  (expect no history change)")

    client.disconnect()
    print("\nDone. Check DB tables.")


if __name__ == "__main__":
    main()
