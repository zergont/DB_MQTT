# Репликация телеметрии на сервер аналитики

Данные из `cg_telemetry` реплицируются на сервер аналитики через PostgreSQL
логическую репликацию. Аналитика получает последние ~7 дней при подключении
и далее получает новые данные в реальном времени.

---

## Часть 1. Ручная настройка на сервере БД (источник)

> Если сервер установлен с флагом `--analytics-ip`, всё ниже уже выполнено.
> Эта секция — для ручной настройки на работающем сервере.

### 1.1 Включить logical replication

```bash
sudo -u postgres psql -d cg_telemetry -c "
    ALTER SYSTEM SET wal_level = 'logical';
    ALTER SYSTEM SET max_replication_slots = 4;
    ALTER SYSTEM SET max_wal_senders = 4;
"
```

### 1.2 Перезапустить PostgreSQL

```bash
sudo systemctl restart postgresql
sudo systemctl restart cg-db-writer
```

Проверить что применилось:

```sql
SHOW wal_level;  -- должно быть: logical
```

### 1.3 Добавить запись в pg_hba.conf

```bash
# Узнать путь к файлу
sudo -u postgres psql -tAc "SHOW hba_file"
# Обычно: /etc/postgresql/16/main/pg_hba.conf

# Добавить строку
echo "host  replication  cg_replicator  10.10.10.7/32  md5" \
    | sudo tee -a /etc/postgresql/16/main/pg_hba.conf

# Применить без перезапуска
sudo -u postgres psql -c "SELECT pg_reload_conf();"
```

### 1.4 Создать пользователя и публикацию

**Через скрипт** (рекомендуется):

```bash
cd /opt/db-writer
venv/bin/python scripts/setup_db.py \
    --config config.yml \
    --setup-replication 10.10.10.7 \
    --replication-password 087475
```

Скрипт идемпотентен — безопасно запускать повторно.

**Или вручную:**

```sql
sudo -u postgres psql -d cg_telemetry

CREATE ROLE cg_replicator WITH REPLICATION LOGIN PASSWORD '087475';

GRANT SELECT ON
    objects, equipment, register_catalog,
    history, events, data_gaps,
    enum_history, fault_history, parameter_history
TO cg_replicator;

CREATE PUBLICATION analytics_pub FOR TABLE
    objects, equipment, register_catalog,
    history, events, data_gaps,
    enum_history, fault_history, parameter_history;
```

### 1.5 Проверить

```sql
sudo -u postgres psql -d cg_telemetry

-- Публикация существует
SELECT pubname, puballtables FROM pg_publication;

-- Таблицы в публикации
SELECT tablename FROM pg_publication_tables WHERE pubname = 'analytics_pub';

-- Слоты репликации (появятся после подключения аналитики)
SELECT slot_name, active FROM pg_replication_slots;
```

---

## Часть 2. Настройка на сервере аналитики

### 2.1 Создать базу данных

```bash
sudo -u postgres createdb cg_analytics
```

### 2.2 Применить схему

```bash
# Скопировать файл с сервера БД или из репозитория
psql -U postgres -d cg_analytics -f schema/schema_analytics.sql
```

### 2.3 Создать подписку

```sql
psql -U postgres -d cg_analytics

CREATE SUBSCRIPTION analytics_sub
    CONNECTION 'host=10.10.10.1 port=5432 dbname=cg_telemetry
                user=cg_replicator password=087475'
    PUBLICATION analytics_pub;
```

PostgreSQL автоматически:
1. Скопирует все существующие данные (последние ~7 дней из `history`,
   все справочники и события)
2. Начнёт получать новые изменения в реальном времени

### 2.4 Проверить статус

```sql
-- Статус подписки (latest_end_lsn должен расти)
SELECT subname, received_lsn, latest_end_lsn, last_msg_receipt_time
FROM pg_stat_subscription;

-- Количество строк в основных таблицах
SELECT 'objects'       AS tbl, count(*) FROM objects
UNION ALL SELECT 'history',        count(*) FROM history
UNION ALL SELECT 'events',         count(*) FROM events
UNION ALL SELECT 'fault_history',  count(*) FROM fault_history
UNION ALL SELECT 'enum_history',   count(*) FROM enum_history;
```

---

## Полезные запросы на аналитике

```sql
-- Обогащённые данные с именами регистров
SELECT *
FROM history_rich
WHERE router_sn = 'XXXX'
  AND ts > now() - INTERVAL '24 hours'
ORDER BY ts DESC;

-- Аварии за период с названиями
SELECT
    f.router_sn,
    f.equip_type,
    f.bit,
    rc.name_default  AS register_name,
    f.fault_start,
    f.fault_end,
    f.fault_end - f.fault_start AS duration
FROM fault_history f
JOIN register_catalog rc
    ON rc.equip_type = f.equip_type
   AND rc.addr       = f.addr
WHERE f.fault_start > now() - INTERVAL '7 days'
ORDER BY f.fault_start DESC;

-- Время работы по объектам (по событиям online/offline)
SELECT
    router_sn,
    type,
    count(*) AS cnt,
    min(created_at) AS first,
    max(created_at) AS last
FROM events
WHERE created_at > now() - INTERVAL '30 days'
GROUP BY router_sn, type
ORDER BY router_sn, type;
```
