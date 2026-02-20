# CG DB-Writer — Модуль хранения телеметрии

**Проект:** «Честная генерация»  
**Назначение:** подписка на MQTT-брокер, приём GPS и decoded-телеметрии панелей PCC, сохранение в PostgreSQL.

## Что делает DB-Writer

| Входные данные | Топик MQTT | Что пишет в БД |
|---|---|---|
| GPS объекта | `cg/v1/telemetry/SN/<router_sn>` | `gps_raw_history` (все точки), `gps_latest_filtered` (стабильная) |
| Decoded регистры панели | `cg/v1/decoded/SN/<router_sn>/pcc/<panel_id>` | `latest_state` (срез), `history` (по правилам), `events` |

### Ключевые принципы
- **latest_state** — фиксированный объём, всегда актуальный срез.
- **history** — только изменения (с tolerance/deadband) + heartbeat для KPI.
- **events** — offline/online переходы, GPS reject, unknown register.
- **GPS anti-teleport** — фильтрация скачков с confirm-буфером.
- БД не раздувается: retention автоматически чистит старые данные.

---

## Требования

- **OS:** Ubuntu 22.04 / 24.04
- **Python:** 3.13+
- **PostgreSQL:** 14+
- **MQTT-брокер:** Mosquitto или совместимый (доступ по сети)

---

## Установка и запуск

### Быстрая установка (Ubuntu, один скрипт)

```bash
git clone https://github.com/zergont/DB_MQTT.git /opt/cg-db-writer
cd /opt/cg-db-writer
sudo chmod +x scripts/install.sh
sudo ./scripts/install.sh
```

Скрипт создаст venv, установит зависимости, скопирует systemd unit-файлы.  
После этого нужно выполнить шаги 1–3 ниже.

### Пошаговая установка (вручную)

#### 1. Клонировать и настроить

```bash
cd /opt
git clone https://github.com/zergont/DB_MQTT.git cg-db-writer
cd cg-db-writer

python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp config.example.yml config.yml
nano config.yml    # ← заполнить mqtt и postgres секции
```

#### 2. Подготовить PostgreSQL

```bash
sudo -u postgres psql
```
```sql
CREATE USER cg_writer WITH PASSWORD 'your_password';
CREATE DATABASE cg_telemetry OWNER cg_writer;
\q
```

Применить схему — **автоматически через скрипт:**
```bash
source venv/bin/activate
python scripts/setup_db.py --config config.yml
```

Или вручную:
```bash
PGPASSWORD=your_password psql -h localhost -U cg_writer -d cg_telemetry -f schema/001_init.sql
```

#### 3. Проверить подключения

```bash
python scripts/check_health.py --config config.yml
```

Ожидаемый вывод:
```
=============================================================
PostgreSQL
=============================================================
  host: localhost:5432  db: cg_telemetry  user: cg_writer
  Подключение: OK
  Таблицы: все на месте
=============================================================
MQTT
=============================================================
  host: localhost:1883
  Подключение: OK
=============================================================
ИТОГО
  PostgreSQL:  OK
  MQTT:        OK
  Всё в порядке! DB-Writer может работать.
```

#### 4. Запуск в foreground (для теста)

```bash
source venv/bin/activate
python -m src --config config.yml
```

Ожидаемый вывод:
```
2025-01-01 12:00:00  INFO     [cg.config]  Config loaded from config.yml
2025-01-01 12:00:00  INFO     [cg.db]      PG pool created  min=2 max=10
2025-01-01 12:00:00  INFO     [cg.main]    Restored GPS state for 0 objects
2025-01-01 12:00:00  INFO     [cg.main]    Connecting to MQTT localhost:1883 …
2025-01-01 12:00:00  INFO     [cg.main]    MQTT connected, subscribed: …
2025-01-01 12:00:00  INFO     [cg.watchdog] Watchdog started, interval=30s
2025-01-01 12:00:00  INFO     [cg.retention] Retention task started: …
```

Остановка: `Ctrl+C`.

#### 5. Запуск как systemd service (production)

```bash
sudo cp systemd/cg-db-writer.service /etc/systemd/system/
sudo cp systemd/cg-db-writer-cleanup.service /etc/systemd/system/
sudo cp systemd/cg-db-writer-cleanup.timer /etc/systemd/system/
sudo useradd -r -s /usr/sbin/nologin cg 2>/dev/null || true

sudo systemctl daemon-reload
sudo systemctl enable --now cg-db-writer.service

# (Опционально) Если хотите вынести очистку в systemd timer:
# sudo systemctl enable --now cg-db-writer-cleanup.timer
```

Проверка:
```bash
sudo systemctl status cg-db-writer
sudo journalctl -u cg-db-writer -f
```

---

## Как понять что работает

### Быстрая проверка (одна команда)

```bash
python scripts/check_health.py --config config.yml
```

Скрипт проверит подключения к PostgreSQL и MQTT, покажет количество строк в каждой таблице и свежесть данных. Если всё зелёное и счётчики растут — работает.

### Проверка через логи

```bash
# Если запущен как systemd service:
sudo journalctl -u cg-db-writer -f

# Если запущен в foreground — логи идут в stdout
```

Что искать в логах:
- `MQTT connected, subscribed` — подключился к брокеру
- `GPS ... accepted=True` — GPS точки принимаются (level=DEBUG)
- `Decoded .../pcc/...: N regs, M history writes` — decoded обрабатывается (level=DEBUG)
- `Watchdog started` — мониторинг offline/online запущен
- `Retention task started` — очистка запланирована

### Проверка через SQL

```sql
-- Сколько данных всего?
SELECT 'objects' AS tbl, count(*) FROM objects
UNION ALL SELECT 'gps_raw_history', count(*) FROM gps_raw_history
UNION ALL SELECT 'gps_latest_filtered', count(*) FROM gps_latest_filtered
UNION ALL SELECT 'latest_state', count(*) FROM latest_state
UNION ALL SELECT 'history', count(*) FROM history
UNION ALL SELECT 'events', count(*) FROM events;

-- Последние GPS точки
SELECT router_sn, accepted, reject_reason, lat, lon, received_at
FROM gps_raw_history ORDER BY id DESC LIMIT 10;

-- Актуальный срез регистров
SELECT router_sn, panel_id, addr, name, value, unit, reason, updated_at
FROM latest_state ORDER BY updated_at DESC LIMIT 20;

-- Последние события
SELECT type, router_sn, description, created_at
FROM events ORDER BY id DESC LIMIT 10;
```

### Тестовая отправка сообщений

```bash
python scripts/test_publish.py --host <mqtt_host> --sn TEST001
```

Скрипт отправит 4 тестовых сообщения (GPS normal, GPS teleport, decoded с 3 регистрами, decoded повтор) и напечатает что проверять.

---

## Проверка работы

### A) Проверка GPS

Установите тестовые зависимости:
```bash
pip install paho-mqtt
```

**Отправить нормальную GPS точку:**
```bash
mosquitto_pub -t "cg/v1/telemetry/SN/6003790403" -m '{
  "GPS": {
    "latitude": 59.851624,
    "longitude": 30.479838,
    "satellites": 8,
    "fix_status": 1,
    "timestamp": 1700000000,
    "date_iso_8601": "2025-01-01T12:00:00+0300"
  }
}'
```

**Проверить SQL:**
```sql
-- Должна быть запись с accepted=true
SELECT * FROM gps_raw_history WHERE router_sn = '6003790403' ORDER BY id DESC LIMIT 5;

-- Должна появиться стабильная точка
SELECT * FROM gps_latest_filtered WHERE router_sn = '6003790403';
```

**Отправить «телепорт» (Москва вместо СПб):**
```bash
mosquitto_pub -t "cg/v1/telemetry/SN/6003790403" -m '{
  "GPS": {
    "latitude": 55.751244,
    "longitude": 37.618423,
    "satellites": 10,
    "fix_status": 1,
    "timestamp": 1700000060,
    "date_iso_8601": "2025-01-01T12:01:00+0300"
  }
}'
```

**Проверить SQL:**
```sql
-- Должна быть запись с accepted=false, reject_reason='jump_distance'
SELECT accepted, reject_reason, lat, lon
FROM gps_raw_history
WHERE router_sn = '6003790403'
ORDER BY id DESC LIMIT 5;

-- gps_latest_filtered НЕ изменился
SELECT * FROM gps_latest_filtered WHERE router_sn = '6003790403';

-- Должен быть event (если enable_gps_reject_events=true)
SELECT * FROM events WHERE router_sn = '6003790403' AND type = 'gps_jump_rejected';
```

### B) Проверка decoded

**Отправить decoded с 3 регистрами:**
```bash
mosquitto_pub -t "cg/v1/decoded/SN/6003790403/pcc/1" -m '{
  "timestamp": "2025-01-01T12:00:00+0300",
  "router_sn": "6003790403",
  "bserver_id": 1,
  "registers": [
    {"addr": 40034, "name": "GensetTotal kW", "value": 150.5, "text": "150.5", "unit": "kW", "raw": 1505, "reason": null},
    {"addr": 40062, "name": "OilPressure", "value": null, "text": null, "unit": "kPa", "raw": null, "reason": "Значение NA"},
    {"addr": 49999, "name": null, "value": 42, "text": "42", "unit": null, "raw": 42, "reason": "Неизвестный регистр"}
  ]
}'
```

**Проверить SQL:**
```sql
-- latest_state обновился для 3 регистров
SELECT addr, name, value, raw, reason
FROM latest_state
WHERE router_sn = '6003790403' AND panel_id = 1;

-- history — должны быть записи (первый раз = change)
SELECT addr, value, write_reason
FROM history
WHERE router_sn = '6003790403' AND panel_id = 1
ORDER BY id DESC LIMIT 10;

-- event для unknown register (если enable_unknown_register_events=true)
SELECT * FROM events WHERE type = 'unknown_register';
```

**Проверить heartbeat KPI:** Подождите `heartbeat_sec` (60 сек в конфиге для KPI) без изменения значений, затем:
```sql
SELECT addr, write_reason, ts
FROM history
WHERE router_sn = '6003790403' AND addr IN (40034, 40062)
ORDER BY id DESC LIMIT 10;
-- Должны появиться записи с write_reason='heartbeat'
```

### C) Проверка events (offline/online)

1. Убедитесь, что DB-Writer работает и получает сообщения.
2. Остановите публикацию (выключите декодер или брокер).
3. Подождите `router_offline_sec` (300 сек по умолчанию).

```sql
SELECT * FROM events WHERE type IN ('router_offline', 'router_online')
ORDER BY created_at DESC LIMIT 10;
```

4. Возобновите публикацию — должен появиться event `router_online`.

### D) Проверка retention

**Ручной запуск очистки:**
```bash
python -m src --config config.yml --cleanup
```

**(Опционально) через systemd timer (если вы его включили):**
```bash
sudo systemctl start cg-db-writer-cleanup.service
sudo journalctl -u cg-db-writer-cleanup -n 20
```

**SQL-проверка:**
```sql
-- До очистки
SELECT count(*) FROM gps_raw_history WHERE received_at < now() - interval '72 hours';
SELECT count(*) FROM history WHERE received_at < now() - interval '30 days';
SELECT count(*) FROM events WHERE created_at < now() - interval '90 days';

-- После очистки — все три запроса должны вернуть 0
```

### E) Автоматический тест (скрипт)

```bash
python scripts/test_publish.py --host localhost --sn TEST001
```

Скрипт отправит 4 сообщения (GPS normal, GPS teleport, decoded, decoded repeat) и выведет подсказки.

---

## Структура проекта

```
cg-db-writer/
├── config.example.yml          # Шаблон конфигурации (без секретов)
├── requirements.txt            # Python зависимости
├── .gitignore                  # config.yml, env/, __pycache__/ и т.д.
├── schema/
│   └── 001_init.sql            # SQL схема — все таблицы и индексы
├── src/
│   ├── __init__.py
│   ├── __main__.py             # python -m src
│   ├── main.py                 # Точка входа, event loop, MQTT
│   ├── config.py               # Загрузка config.yml
│   ├── log.py                  # Настройка логирования
│   ├── db.py                   # Все SQL операции (asyncpg)
│   ├── handlers.py             # Обработка MQTT сообщений
│   ├── gps_filter.py           # GPS anti-teleport фильтр
│   ├── history_policy.py       # Логика «писать ли в history»
│   ├── watchdog.py             # Мониторинг online/offline/stale
│   └── retention.py            # Очистка устаревших данных
├── scripts/
│   ├── install.sh              # Автоустановка на Ubuntu
│   ├── setup_db.py             # Применение SQL схемы
│   ├── check_health.py         # Проверка подключений и данных
│   ├── test_publish.py         # Тестовая публикация в MQTT
│   └── smoke_test.py           # Локальный тест без MQTT/Postgres
├── systemd/
│   ├── cg-db-writer.service    # systemd unit
│   ├── cg-db-writer-cleanup.service
│   └── cg-db-writer-cleanup.timer
└── README.md
```

---

## Конфигурация

Все настройки — в `config.yml`. Подробное описание каждой секции — в `config.example.yml` (с комментариями).

Ключевые секции:

| Секция | Что настраивает |
|---|---|
| `mqtt` | Подключение к брокеру, топики подписки |
| `postgres` | Подключение к БД, размер пула |
| `gps_filter` | Пороги anti-teleport (sats, fix, jump, speed, confirm) |
| `history_policy` | Tolerance, min_interval, heartbeat, KPI регистры |
| `events_policy` | Пороги stale/offline, включение GPS/unknown events |
| `retention` | Сроки хранения (gps 72h, history 30d, events 90d) |
| `logging` | Уровень, файл, JSON формат |

---

## Troubleshooting

### MQTT не коннектится
```
MQTT connection lost: [Errno 111] Connection refused
```
- Проверьте host/port в `config.yml`
- Проверьте что Mosquitto запущен: `systemctl status mosquitto`
- Проверьте user/password

### PostgreSQL не коннектится
```
asyncpg.InvalidPasswordError
```
- Проверьте host/port/dbname/user/password в `config.yml`
- Проверьте `pg_hba.conf` (разрешён ли md5/scram для вашего пользователя)

### History «не растёт»
- Значение не изменилось больше чем `tolerance` — это нормально.
- `min_interval_sec` не прошёл — подождите.
- `heartbeat_sec` ещё не наступил — проверьте настройку (для KPI — 60 сек).
- `store_history: false` в `register_catalog` — регистр исключён из истории.

### GPS всё время rejected
```sql
SELECT reject_reason, count(*) FROM gps_raw_history
WHERE router_sn = '...' GROUP BY reject_reason;
```
- `low_sats` → уменьшите `sats_min` (по умолчанию 4)
- `bad_fix` → уменьшите `fix_min`
- `jump_distance` / `jump_speed` → увеличьте `max_jump_m` / `max_speed_kmh`
- Если объект реально переехал → дождитесь `confirm_points` (по умолчанию 3 точки в радиусе 50м)

### Логи
```bash
# systemd
sudo journalctl -u cg-db-writer -f

# foreground с debug
# В config.yml: logging.level: DEBUG
python -m src --config config.yml
```
