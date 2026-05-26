# CG Analytics — руководство по работе с БД

**База данных:** PostgreSQL + TimescaleDB  
**БД:** `cg_telemetry`  
**Роль:** `cg_analytics` (только SELECT)

---

## Подключение

```
host:     <сервер>
port:     5432
dbname:   cg_telemetry
user:     cg_analytics
password: cg_analytics_pass
```

asyncpg (Python):
```python
conn = await asyncpg.connect(
    host="...", port=5432,
    database="cg_telemetry",
    user="cg_analytics", password="cg_analytics_pass",
)
```

---

## Структура данных

### Оборудование

#### `objects` — роутеры (верхний уровень)

| Колонка | Тип | Описание |
|---|---|---|
| `router_sn` | TEXT PK | серийник роутера |
| `name` | TEXT | название объекта |
| `notes` | TEXT | примечания |
| `created_at` | TIMESTAMPTZ | первое появление |
| `updated_at` | TIMESTAMPTZ | последнее обновление |

#### `equipment` — панели управления

| Колонка | Тип | Описание |
|---|---|---|
| `router_sn` | TEXT FK | серийник роутера |
| `equip_type` | TEXT | тип (`pcc`, …) |
| `panel_id` | INT | ID панели |
| `name` | TEXT | название |
| `manufacturer` | TEXT | производитель |
| `model` | TEXT | модель |
| `engine_sn` | TEXT | серийник двигателя |
| `first_seen_at` | TIMESTAMPTZ | первое появление |
| `last_seen_at` | TIMESTAMPTZ | последнее появление |

```sql
-- Список всего оборудования
SELECT o.name AS object_name, e.router_sn, e.equip_type, e.panel_id,
       e.name, e.manufacturer, e.model, e.engine_sn,
       e.first_seen_at, e.last_seen_at
FROM equipment e
JOIN objects o USING (router_sn)
ORDER BY o.name, e.equip_type, e.panel_id;
```

---

### Каталог регистров

#### `register_catalog` — метаданные регистров из MQTT map

| Колонка | Тип | Описание |
|---|---|---|
| `equip_type` | TEXT | тип оборудования |
| `addr` | INT | адрес регистра (Modbus) |
| `name_default` | TEXT | название (English) |
| `name_ru` | TEXT | название (русский) |
| `unit_default` | TEXT | единица (`kW`, `bar`, `enum`, `fault_bitmap`, …) |
| `register_kind` | TEXT | `analog` / `enum` / `fault_bitmap` |
| `states_json` | JSONB | расшифровка состояний (для enum и fault_bitmap) |

**`states_json` по типам:**

```jsonc
// enum
{
  "labels":    {"0": "Off",  "1": "Auto",  "2": "Manual"},
  "labels_ru": {"0": "Выкл","1": "Авто",  "2": "Ручной"}  // если есть перевод
}

// fault_bitmap
{
  "0": {"name": "Low Oil Pressure", "name_ru": "Низкое давление масла", "severity": "shutdown"},
  "1": {"name": "High Temp",        "severity": "warning"}  // name_ru опционально
}

// analog → null
```

```sql
-- Все регистры типа оборудования с расшифровкой
SELECT addr, name_default, name_ru, unit_default, register_kind, states_json
FROM register_catalog
WHERE equip_type = 'pcc'
ORDER BY addr;
```

---

### История значений (телеметрия)

#### `history_rich` — обогащённая история (рекомендуется)

VIEW = `history LEFT JOIN register_catalog`. Содержит данные + метаданные регистра.

| Колонка | Описание |
|---|---|
| `router_sn` | серийник роутера |
| `equip_type` | тип оборудования |
| `panel_id` | ID панели |
| `addr` | адрес регистра |
| `ts` | device-time (время на устройстве) |
| `received_at` | server-time (время получения) |
| `value` | числовое значение (NUMERIC) |
| `raw` | сырое целое (INT) |
| `name` | название регистра (English) |
| `unit` | единица измерения |
| `register_kind` | тип регистра |
| `states_json` | расшифровка состояний |
| `name_ru` | название регистра (русский) |

```sql
-- История конкретного оборудования за период
SELECT ts, addr, name_ru, value, unit, register_kind
FROM history_rich
WHERE router_sn  = 'EMURKL2X'
  AND equip_type = 'pcc'
  AND panel_id   = 1
  AND ts BETWEEN '2024-01-01' AND '2024-01-02'
ORDER BY ts, addr;

-- Конкретный регистр за период
SELECT ts, value, raw
FROM history_rich
WHERE router_sn  = 'EMURKL2X'
  AND equip_type = 'pcc'
  AND panel_id   = 1
  AND addr       = 40034      -- GensetTotal kW
  AND ts >= now() - INTERVAL '24 hours'
ORDER BY ts;

-- Последнее значение каждого регистра (текущее состояние)
SELECT addr, name_ru, value, unit, ts
FROM history_rich
WHERE router_sn  = 'EMURKL2X'
  AND equip_type = 'pcc'
  AND panel_id   = 1
  AND ts = (
      SELECT max(ts) FROM history
      WHERE router_sn = 'EMURKL2X' AND equip_type = 'pcc'
        AND panel_id = 1 AND addr = history_rich.addr
  )
ORDER BY addr;
```

#### `latest_state` — актуальное значение каждого регистра

```sql
-- Текущие значения всего оборудования с именами
SELECT ls.router_sn, ls.equip_type, ls.panel_id, ls.addr,
       r.name_ru, r.unit_default AS unit,
       ls.value, ls.raw, ls.ts
FROM latest_state ls
LEFT JOIN register_catalog r
    ON r.equip_type = ls.equip_type AND r.addr = ls.addr
WHERE ls.router_sn = 'EMURKL2X'
ORDER BY ls.addr;
```

#### Агрегированная история (TimescaleDB Continuous Aggregates)

```sql
-- 1-минутные агрегаты (хранятся 30 дней)
SELECT bucket, router_sn, equip_type, panel_id, addr,
       avg_value, min_value, max_value, sample_count
FROM history_1min
WHERE router_sn = 'EMURKL2X' AND addr = 40034
  AND bucket >= now() - INTERVAL '7 days'
ORDER BY bucket;

-- 1-часовые агрегаты (хранятся 1 год)
SELECT bucket, avg_value, min_value, max_value
FROM history_1hour
WHERE router_sn = 'EMURKL2X' AND addr = 40034
  AND bucket >= now() - INTERVAL '30 days'
ORDER BY bucket;
```

---

### Журнал состояний (enum)

#### `enum_history` — периоды активности enum-состояний

Каждая запись = один период нахождения регистра в определённом состоянии.  
`state_end IS NULL` = состояние активно прямо сейчас.

| Колонка | Тип | Описание |
|---|---|---|
| `router_sn` | TEXT | серийник роутера |
| `equip_type` | TEXT | тип оборудования |
| `panel_id` | INT | ID панели |
| `addr` | INT | адрес регистра |
| `value` | INT | код состояния (raw) |
| `state_start` | TIMESTAMPTZ | начало состояния |
| `state_end` | TIMESTAMPTZ | конец состояния (`NULL` = текущее) |

```sql
-- Журнал состояний с расшифровкой (рекомендуемый запрос)
SELECT
    e.router_sn, e.equip_type, e.panel_id, e.addr,
    r.name_ru                                       AS register_name,
    e.value,
    r.states_json->'labels_ru'->>e.value::text      AS label_ru,
    r.states_json->'labels'   ->>e.value::text      AS label,
    e.state_start,
    e.state_end,
    EXTRACT(EPOCH FROM (
        COALESCE(e.state_end, now()) - e.state_start
    ))                                              AS duration_sec
FROM enum_history e
LEFT JOIN register_catalog r
    ON r.equip_type = e.equip_type AND r.addr = e.addr
WHERE e.router_sn  = 'EMURKL2X'
  AND e.equip_type = 'pcc'
  AND e.panel_id   = 1
  AND e.state_start >= now() - INTERVAL '24 hours'
ORDER BY e.addr, e.state_start;

-- Текущие состояния (открытые периоды)
SELECT
    e.router_sn, e.addr,
    r.name_ru,
    e.value,
    r.states_json->'labels_ru'->>e.value::text AS label_ru,
    e.state_start,
    EXTRACT(EPOCH FROM (now() - e.state_start)) AS duration_sec
FROM enum_history e
LEFT JOIN register_catalog r
    ON r.equip_type = e.equip_type AND r.addr = e.addr
WHERE e.router_sn  = 'EMURKL2X'
  AND e.equip_type = 'pcc'
  AND e.panel_id   = 1
  AND e.state_end IS NULL
ORDER BY e.addr;

-- Сколько времени в каждом состоянии за период (аналитика)
SELECT
    r.name_ru                                       AS register_name,
    e.value,
    r.states_json->'labels_ru'->>e.value::text      AS label_ru,
    SUM(EXTRACT(EPOCH FROM (
        LEAST(COALESCE(e.state_end, now()), '2024-02-01'::timestamptz)
        - GREATEST(e.state_start, '2024-01-01'::timestamptz)
    )))                                             AS total_sec
FROM enum_history e
LEFT JOIN register_catalog r
    ON r.equip_type = e.equip_type AND r.addr = e.addr
WHERE e.router_sn  = 'EMURKL2X'
  AND e.equip_type = 'pcc'
  AND e.panel_id   = 1
  AND e.state_start < '2024-02-01'
  AND (e.state_end IS NULL OR e.state_end > '2024-01-01')
GROUP BY r.name_ru, e.value, label_ru
ORDER BY r.name_ru, e.value;
```

---

### Журнал неисправностей

#### `fault_history` — периоды активности fault-битов

| Колонка | Тип | Описание |
|---|---|---|
| `router_sn` | TEXT | серийник роутера |
| `equip_type` | TEXT | тип оборудования |
| `panel_id` | INT | ID панели |
| `addr` | INT | адрес fault_bitmap регистра |
| `bit` | INT | номер бита (0-15) |
| `fault_start` | TIMESTAMPTZ | начало неисправности |
| `fault_end` | TIMESTAMPTZ | конец неисправности (`NULL` = активна) |

```sql
-- Журнал неисправностей с расшифровкой
SELECT
    f.router_sn, f.equip_type, f.panel_id, f.addr, f.bit,
    r.states_json->f.bit::text->>'name_ru'    AS fault_name_ru,
    r.states_json->f.bit::text->>'name'       AS fault_name,
    r.states_json->f.bit::text->>'severity'   AS severity,
    f.fault_start,
    f.fault_end,
    EXTRACT(EPOCH FROM (
        COALESCE(f.fault_end, now()) - f.fault_start
    ))                                        AS duration_sec
FROM fault_history f
LEFT JOIN register_catalog r
    ON r.equip_type = f.equip_type AND r.addr = f.addr
WHERE f.router_sn  = 'EMURKL2X'
  AND f.equip_type = 'pcc'
  AND f.panel_id   = 1
ORDER BY f.fault_start DESC
LIMIT 100;

-- Только активные неисправности
SELECT
    f.router_sn, f.addr, f.bit,
    r.states_json->f.bit::text->>'name_ru'  AS fault_name_ru,
    r.states_json->f.bit::text->>'severity' AS severity,
    f.fault_start,
    EXTRACT(EPOCH FROM (now() - f.fault_start)) AS active_sec
FROM fault_history f
LEFT JOIN register_catalog r
    ON r.equip_type = f.equip_type AND r.addr = f.addr
WHERE f.fault_end IS NULL
ORDER BY f.fault_start;

-- Статистика неисправностей за период (топ по частоте)
SELECT
    f.addr, f.bit,
    r.states_json->f.bit::text->>'name_ru'  AS fault_name_ru,
    r.states_json->f.bit::text->>'severity' AS severity,
    COUNT(*)                                AS occurrences,
    SUM(EXTRACT(EPOCH FROM (
        COALESCE(f.fault_end, now()) - f.fault_start
    )))                                     AS total_sec
FROM fault_history f
LEFT JOIN register_catalog r
    ON r.equip_type = f.equip_type AND r.addr = f.addr
WHERE f.router_sn  = 'EMURKL2X'
  AND f.equip_type = 'pcc'
  AND f.fault_start >= now() - INTERVAL '30 days'
GROUP BY f.addr, f.bit, fault_name_ru, severity
ORDER BY occurrences DESC;
```

---

### Системные события

#### `events` — события роутеров и оборудования

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGSERIAL | ID |
| `router_sn` | TEXT | серийник роутера |
| `equip_type` | TEXT | тип оборудования (`NULL` = событие роутера) |
| `panel_id` | INT | ID панели |
| `event_type` | TEXT | тип события |
| `description` | TEXT | описание |
| `payload` | JSONB | дополнительные данные |
| `ts` | TIMESTAMPTZ | время события |

Типичные `event_type`:
- `router_stale` / `router_offline` / `router_online`
- `panel_stale` / `panel_offline` / `panel_online`
- `fault` — появление неисправности
- `gps_jump_rejected` — отклонённая GPS-точка

```sql
-- Последние события
SELECT ts, router_sn, equip_type, panel_id, event_type, description
FROM events
WHERE router_sn = 'EMURKL2X'
  AND ts >= now() - INTERVAL '7 days'
ORDER BY ts DESC
LIMIT 100;

-- Только offline/online события (история связи)
SELECT ts, router_sn, equip_type, panel_id, event_type
FROM events
WHERE router_sn IN ('EMURKL2X', 'EMU8ZVUK')
  AND event_type IN ('router_offline', 'router_online',
                     'panel_offline',  'panel_online')
ORDER BY ts DESC;
```

---

### Пропуски связи

#### `data_gaps` — зафиксированные gap'ы (потери пакетов)

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGSERIAL | ID |
| `router_sn` | TEXT | серийник роутера |
| `equip_type` | TEXT | тип оборудования |
| `panel_id` | INT | ID панели |
| `gap_start` | TIMESTAMPTZ | начало пропуска |
| `gap_end` | TIMESTAMPTZ | конец пропуска (`NULL` = ещё не закрыт) |

```sql
-- Журнал пропусков связи
SELECT router_sn, equip_type, panel_id,
       gap_start, gap_end,
       EXTRACT(EPOCH FROM (
           COALESCE(gap_end, now()) - gap_start
       ))/60 AS gap_minutes
FROM data_gaps
WHERE router_sn = 'EMURKL2X'
  AND gap_start >= now() - INTERVAL '30 days'
ORDER BY gap_start DESC;
```

---

### GPS

#### `gps_latest_filtered` — последняя валидная позиция

```sql
SELECT router_sn, lat, lon, satellites, fix_status, gps_time
FROM gps_latest_filtered
ORDER BY router_sn;
```

#### `gps_raw_history` — полная история GPS (хранится 3 дня)

```sql
SELECT router_sn, gps_time, lat, lon, satellites, fix_status, accepted, reject_reason
FROM gps_raw_history
WHERE router_sn = 'EMURKL2X'
  AND gps_time >= now() - INTERVAL '24 hours'
ORDER BY gps_time DESC;
```

---

## Типовые аналитические запросы

### Наработка (суммарное время в рабочем состоянии)

```sql
-- Время в состоянии "Auto" (value=1) за месяц
SELECT
    SUM(EXTRACT(EPOCH FROM (
        LEAST(COALESCE(state_end, now()), date_trunc('month', now()) + INTERVAL '1 month')
        - GREATEST(state_start, date_trunc('month', now()))
    )))/3600 AS hours_in_auto
FROM enum_history
WHERE router_sn  = 'EMURKL2X'
  AND equip_type = 'pcc'
  AND panel_id   = 1
  AND addr       = 40010      -- Control Switch Position
  AND value      = 1          -- Auto
  AND state_start < date_trunc('month', now()) + INTERVAL '1 month'
  AND (state_end IS NULL OR state_end > date_trunc('month', now()));
```

### Тренд нагрузки (1-часовые агрегаты)

```sql
SELECT bucket,
       avg_value AS avg_kw,
       max_value AS peak_kw,
       min_value AS min_kw
FROM history_1hour
WHERE router_sn  = 'EMURKL2X'
  AND equip_type = 'pcc'
  AND panel_id   = 1
  AND addr       = 40034      -- GensetTotal kW
  AND bucket >= now() - INTERVAL '30 days'
ORDER BY bucket;
```

### Доступность оборудования за период

```sql
-- Время без gap'ов / общее время × 100%
WITH period AS (
    SELECT '2024-01-01'::timestamptz AS t_from,
           '2024-02-01'::timestamptz AS t_to
),
gaps AS (
    SELECT SUM(EXTRACT(EPOCH FROM (
        LEAST(COALESCE(gap_end, p.t_to), p.t_to)
        - GREATEST(gap_start, p.t_from)
    ))) AS gap_sec
    FROM data_gaps, period p
    WHERE router_sn  = 'EMURKL2X'
      AND equip_type = 'pcc'
      AND panel_id   = 1
      AND gap_start < p.t_to
      AND (gap_end IS NULL OR gap_end > p.t_from)
)
SELECT
    100.0 * (1 - COALESCE(gap_sec, 0) /
        EXTRACT(EPOCH FROM (p.t_to - p.t_from))
    ) AS availability_pct
FROM gaps, period p;
```

---

## Retention (сколько данных хранится)

| Таблица | Срок хранения |
|---|---|
| `history` (raw) | 30 дней |
| `history_1min` | 30 дней |
| `history_1hour` | 1 год |
| `gps_raw_history` | 3 дня |
| `enum_history` | без ограничений |
| `fault_history` | без ограничений |
| `events` | без ограничений |
| `data_gaps` | без ограничений |
