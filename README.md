# CG DB-Writer — модуль хранения телеметрии (MQTT → PostgreSQL)

Проект: **«Честная генерация»**  
Назначение: подписка на MQTT-брокер, приём **GPS объекта** и **decoded-телеметрии панелей (PCC 3.3)**, сохранение в **PostgreSQL** с политикой, чтобы база **не раздувалась**.

## Что делает DB-Writer

| Входные данные | Топик MQTT | Что пишет в БД |
|---|---|---|
| GPS объекта | `cg/v1/telemetry/SN/<router_sn>` | `gps_raw_history` (все точки), `gps_latest_filtered` (стабильная точка) |
| Decoded регистры панели | `cg/v1/decoded/SN/<router_sn>/pcc/<panel_id>` | `latest_state` (срез), `history` (по правилам), `events` |

### Ключевые принципы
- **latest_state** — фиксированный объём, всегда актуальный срез.
- **history** — только изменения (tolerance/deadband) + **heartbeat** для KPI.
- **events** — offline/online переходы, GPS reject, unknown register.
- **GPS anti-teleport** — фильтрация скачков координат (confirm-буфер).
- **Retention** — автоматическая очистка старых данных.

## Требования
- **OS:** Ubuntu 22.04 / 24.04
- **Python:** 3.13+
- **PostgreSQL:** 14+
- **MQTT-брокер:** Mosquitto или совместимый

## Конфигурация
Все настройки в одном файле: `config.yml` (в репозитории хранится только шаблон `config.example.yml`).

Ключевые секции:
- `mqtt` — host/port/учётка + топики подписки (`decoded`, `telemetry`)
- `postgres` — подключение к БД + размеры пула
- `gps_filter` — пороги anti-teleport
- `history_policy` — tolerance/min_interval/heartbeat + KPI addr
- `events_policy` — stale/offline пороги + включение отдельных событий
- `retention` — сроки хранения и батч очистки
- `logging` — уровни/файл/JSON-логи

## Установка и запуск

### Быстрая установка (Ubuntu, один скрипт)
```bash
git clone https://github.com/zergont/DB_MQTT.git /opt/cg-db-writer
cd /opt/cg-db-writer
sudo chmod +x scripts/install.sh
sudo ./scripts/install.sh
```

Скрипт создаст venv, установит зависимости, поставит systemd unit-файлы.  
Далее выполните шаги 1–3 ниже (создать БД, применить схему, health-check).

### Пошаговая установка (вручную)

#### 1) Клонировать и подготовить venv
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

#### 2) Подготовить PostgreSQL
```bash
sudo -u postgres psql
```

```sql
CREATE USER cg_writer WITH PASSWORD 'your_password';
CREATE DATABASE cg_telemetry OWNER cg_writer;
\q
```

Применить схему (рекомендуется скриптом):
```bash
source venv/bin/activate
python scripts/setup_db.py --config config.yml
```

Или вручную:
```bash
PGPASSWORD=your_password psql -h localhost -U cg_writer -d cg_telemetry -f schema/001_init.sql
```

#### 3) Проверить подключения (health-check)
```bash
python scripts/check_health.py --config config.yml
```

Ожидаемо: PostgreSQL OK, MQTT OK.

#### 4) Запуск в foreground (для теста)
```bash
source venv/bin/activate
python -m src --config config.yml
```

Остановка: `Ctrl+C`.

#### 5) Запуск как systemd service (production)
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

> Примечание: в сервисе уже есть встроенный retention-loop. Timer нужен только если вы сознательно хотите чистку отдельно.

## Как понять, что работает

### 1) Быстрая проверка
```bash
python scripts/check_health.py --config config.yml
```

### 2) Проверка через SQL
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
FROM gps_raw_history
ORDER BY id DESC
LIMIT 10;

-- Актуальный срез регистров
SELECT router_sn, equip_type, panel_id, addr, name, value, unit, reason, updated_at
FROM latest_state
ORDER BY updated_at DESC
LIMIT 20;

-- Последние события
SELECT type, router_sn, equip_type, panel_id, description, created_at
FROM events
ORDER BY id DESC
LIMIT 10;
```

### 3) Тестовая отправка сообщений
```bash
python scripts/test_publish.py --host <MQTT_HOST> --sn TEST001
```

## Нагрузка и надёжность (под ваш кейс)

Ожидаемая нагрузка: **10 панелей × до 6 устройств** (≈60 “единиц”), плюс GPS раз в 30–60 сек.  
Эта схема нормально укладывается в PostgreSQL при условии:
- history пишет **не всё подряд**, а по политике (tolerance/min_interval/heartbeat)
- включена очистка (retention)
- пул соединений адекватный

Рекомендации “на надёжность”:
1) **Ограничение входного потока (backpressure)**  
   Если вы ожидаете всплески сообщений или временные тормоза БД — лучший паттерн: `asyncio.Queue(maxsize=...)` и 1–2 DB-воркера.  
   Это защищает процесс от лавины задач и даёт прогнозируемое поведение.
2) **Кэш register_catalog и last_state в памяти**  
   Это уменьшает число SELECT на каждый регистр и повышает устойчивость под нагрузкой.
3) **Circuit breaker / backoff на ошибки БД**  
   При временной недоступности PostgreSQL — логируем и делаем задержку (1/2/5/10 сек), чтобы не устроить DDoS логами и не убить CPU.

Если хотите, я подготовлю короткий “дизайн-док” для этих трёх улучшений (без кода, но с точными шагами и настройками).

## Структура проекта
```text
cg-db-writer/
├── config.example.yml
├── requirements.txt
├── schema/
│   └── 001_init.sql
├── src/
│   ├── main.py
│   ├── handlers.py
│   ├── db.py
│   ├── gps_filter.py
│   ├── history_policy.py
│   ├── watchdog.py
│   └── retention.py
├── scripts/
│   ├── install.sh
│   ├── setup_db.py
│   ├── check_health.py
│   └── test_publish.py
├── systemd/
│   ├── cg-db-writer.service
│   ├── cg-db-writer-cleanup.service
│   └── cg-db-writer-cleanup.timer
└── README.md
```

## Troubleshooting

### MQTT не коннектится
- Проверьте host/port в `config.yml`
- Проверьте брокер: `systemctl status mosquitto`
- Проверьте логин/пароль, TLS

### PostgreSQL не коннектится
- Проверьте host/port/dbname/user/password в `config.yml`
- Проверьте `pg_hba.conf` (md5/scram), доступ с сервера

### History «не растёт»
Это часто **нормально**: политика history режет поток.
- значение не изменилось больше tolerance
- min_interval ещё не прошёл
- heartbeat ещё не наступил (для KPI обычно 60 сек)
