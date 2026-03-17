-- =============================================================================
-- CG DB-Writer v2.0.0 — TimescaleDB schema
--
-- Применить:
--   python scripts/setup_db.py --config config.yml
--
-- Требования:
--   TimescaleDB 2.9+ (для иерархических Continuous Aggregates)
--
-- Роли БД (создаются setup_db.py):
--   cg_writer — DB-Writer (INSERT/SELECT)
--   cg_ui     — UI backend (SELECT)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 0. Расширение
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Справочные таблицы (обычный PostgreSQL)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS objects (
    router_sn   TEXT        PRIMARY KEY,
    name        TEXT,
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS equipment (
    router_sn       TEXT        NOT NULL REFERENCES objects(router_sn) ON DELETE CASCADE,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    name            TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (router_sn, equip_type, panel_id)
);

-- register_kind:
--   analog    — измерение (ток, напряжение, мощность) → history + CA агрегация
--   discrete  — бинарное состояние (вкл/выкл, авария) → state_events
--   enum      — перечислимое состояние (AUTO/MANUAL)   → state_events
--   parameter — уставка / настройка (редко меняется)   → parameter_history
CREATE TABLE IF NOT EXISTS register_catalog (
    equip_type       TEXT        NOT NULL,
    addr             INT         NOT NULL,
    name_default     TEXT,
    unit_default     TEXT,
    register_kind    TEXT        NOT NULL DEFAULT 'analog'
                     CHECK (register_kind IN ('analog', 'discrete', 'enum', 'parameter')),
    value_kind       TEXT        NOT NULL DEFAULT 'analog',
    tolerance        NUMERIC,
    min_interval_sec INT,
    heartbeat_sec    INT,
    store_history    BOOL        NOT NULL DEFAULT true,
    PRIMARY KEY (equip_type, addr)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. GPS (TimescaleDB hypertable — частые вставки, retention policy)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gps_raw_history (
    router_sn       TEXT              NOT NULL,
    received_at     TIMESTAMPTZ       NOT NULL DEFAULT now(),
    gps_time        TIMESTAMPTZ,
    lat             DOUBLE PRECISION  NOT NULL,
    lon             DOUBLE PRECISION  NOT NULL,
    satellites      INT,
    fix_status      INT,
    accepted        BOOL              NOT NULL,
    reject_reason   TEXT
);

SELECT create_hypertable(
    'gps_raw_history', 'received_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => true
);

CREATE INDEX IF NOT EXISTS idx_gps_raw_sn_recv
    ON gps_raw_history (router_sn, received_at DESC);

CREATE TABLE IF NOT EXISTS gps_latest_filtered (
    router_sn   TEXT             PRIMARY KEY,
    gps_time    TIMESTAMPTZ,
    received_at TIMESTAMPTZ      NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    satellites  INT,
    fix_status  INT
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Latest state (последнее известное значение каждого регистра)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS latest_state (
    router_sn   TEXT        NOT NULL,
    equip_type  TEXT        NOT NULL,
    panel_id    INT         NOT NULL,
    addr        INT         NOT NULL,
    ts          TIMESTAMPTZ,
    value       NUMERIC,
    raw         INT,
    text        TEXT,
    unit        TEXT,
    name        TEXT,
    reason      TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (router_sn, equip_type, panel_id, addr)
);

CREATE INDEX IF NOT EXISTS idx_latest_state_updated
    ON latest_state (router_sn, equip_type, panel_id, updated_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. History — аналоговые регистры (TimescaleDB hypertable)
--
--    Только register_kind = 'analog'.
--    Записывается по change + heartbeat (для детекции gap'ов).
--    ts — время устройства (NOT NULL: если устройство не присылает ts,
--    используем received_at как fallback на уровне приложения).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS history (
    router_sn       TEXT        NOT NULL,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    addr            INT         NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    value           NUMERIC,
    raw             INT,
    text            TEXT,
    reason          TEXT,
    write_reason    TEXT        NOT NULL   -- 'change' | 'heartbeat'
);

SELECT create_hypertable(
    'history', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => true
);

-- Компрессия: chunks старше 7 дней сжимаются (10-20× экономия места)
ALTER TABLE history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'router_sn, equip_type, panel_id, addr',
    timescaledb.compress_orderby   = 'ts ASC'
);

CREATE INDEX IF NOT EXISTS idx_history_key_ts
    ON history (router_sn, equip_type, panel_id, addr, ts DESC);

CREATE INDEX IF NOT EXISTS idx_history_received_at
    ON history (received_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Continuous Aggregates (TimescaleDB)
--
--    history_1min:  агрегация по 1 минуте из history
--    history_1hour: иерархическая агрегация по 1 часу из history_1min
--
--    Включают open_value / close_value (first/last) — для будущего candlestick.
--    materialized_only = false → real-time данные видны сразу.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS history_1min
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT
    router_sn,
    equip_type,
    panel_id,
    addr,
    time_bucket('1 minute', ts)  AS ts,
    avg(value)                   AS avg_value,
    min(value)                   AS min_value,
    max(value)                   AS max_value,
    first(value, ts)             AS open_value,
    last(value, ts)              AS close_value,
    count(*)                     AS sample_count
FROM history
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

-- Обновлять каждую минуту; start_offset=60min учитывает позднее прибытие данных
SELECT add_continuous_aggregate_policy(
    'history_1min',
    start_offset      => INTERVAL '60 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => true
);

-- Иерархический CA: history_1hour строится поверх history_1min
-- (требует TimescaleDB 2.9+)
CREATE MATERIALIZED VIEW IF NOT EXISTS history_1hour
WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT
    router_sn,
    equip_type,
    panel_id,
    addr,
    time_bucket('1 hour', ts)                                        AS ts,
    -- Weighted average по sample_count (точнее, чем avg(avg_value))
    sum(avg_value * sample_count) / NULLIF(sum(sample_count), 0)    AS avg_value,
    min(min_value)                                                   AS min_value,
    max(max_value)                                                   AS max_value,
    -- Open/close: первый open первой минуты = open часа; last close = close часа
    first(open_value,  ts)                                           AS open_value,
    last(close_value,  ts)                                           AS close_value,
    sum(sample_count)                                                AS sample_count
FROM history_1min
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'history_1hour',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => true
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Retention & Compression policies (TimescaleDB)
-- ─────────────────────────────────────────────────────────────────────────────

-- Сжатие raw history старше 7 дней
SELECT add_compression_policy(
    'history',
    compress_after => INTERVAL '7 days',
    if_not_exists  => true
);

-- Retention raw: 30 дней
SELECT add_retention_policy(
    'history',
    drop_after    => INTERVAL '30 days',
    if_not_exists => true
);

-- Retention 1min CA: 90 дней
SELECT add_retention_policy(
    'history_1min',
    drop_after    => INTERVAL '90 days',
    if_not_exists => true
);

-- Retention 1hour CA: 3 года
SELECT add_retention_policy(
    'history_1hour',
    drop_after    => INTERVAL '3 years',
    if_not_exists => true
);

-- Retention GPS raw: 3 дня
-- ВАЖНО: сначала включаем compress, потом добавляем policy
ALTER TABLE gps_raw_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'router_sn',
    timescaledb.compress_orderby   = 'received_at ASC'
);
SELECT add_compression_policy(
    'gps_raw_history',
    compress_after => INTERVAL '1 day',
    if_not_exists  => true
);
SELECT add_retention_policy(
    'gps_raw_history',
    drop_after    => INTERVAL '3 days',
    if_not_exists => true
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. State events — дискретные/enum регистры (обычный PostgreSQL)
--
--    Отдельная таблица: нет агрегации, только журнал изменений.
--    Heartbeat пишется для детекции gap'ов (пропущенных событий).
--    Gap = интервал между записями > heartbeat_sec * 2.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS state_events (
    id              BIGSERIAL   PRIMARY KEY,
    router_sn       TEXT        NOT NULL,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    addr            INT         NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw             INT,
    text            TEXT,
    write_reason    TEXT        NOT NULL   -- 'change' | 'heartbeat'
);

CREATE INDEX IF NOT EXISTS idx_state_events_key_ts
    ON state_events (router_sn, equip_type, panel_id, addr, ts DESC);

CREATE INDEX IF NOT EXISTS idx_state_events_received_at
    ON state_events (received_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. Parameter history — уставки и настройки (обычный PostgreSQL)
--
--    Пишется только при изменении значения (без heartbeat).
--    Хранится долго — аудит изменений параметров оборудования.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS parameter_history (
    id              BIGSERIAL   PRIMARY KEY,
    router_sn       TEXT        NOT NULL,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    addr            INT         NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    value           NUMERIC,
    raw             INT,
    text            TEXT
);

CREATE INDEX IF NOT EXISTS idx_param_history_key_ts
    ON parameter_history (router_sn, equip_type, panel_id, addr, ts DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. Events — системные события (online/offline/gps_reject и др.)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL   PRIMARY KEY,
    router_sn   TEXT        NOT NULL,
    equip_type  TEXT,
    panel_id    INT,
    type        TEXT        NOT NULL,
    description TEXT,
    payload     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_sn_created
    ON events (router_sn, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_type_created
    ON events (type, created_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 10. Share links (UI)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS share_links (
    id          SERIAL      PRIMARY KEY,
    token_hash  TEXT        NOT NULL UNIQUE,
    label       TEXT        NOT NULL DEFAULT '',
    scope_type  TEXT        NOT NULL DEFAULT 'all'
                CHECK (scope_type IN ('all', 'site', 'device')),
    scope_id    TEXT,
    role        TEXT        NOT NULL DEFAULT 'viewer'
                CHECK (role IN ('viewer', 'admin')),
    max_uses    INT,
    use_count   INT         NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ,
    revoked_at  TIMESTAMPTZ,
    created_by  TEXT        NOT NULL DEFAULT 'admin'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 11. Права доступа
-- ─────────────────────────────────────────────────────────────────────────────

-- cg_ui: чтение всего + запись имён объектов
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'cg_ui') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO cg_ui;
        GRANT UPDATE (name, notes) ON objects TO cg_ui;
        GRANT UPDATE (name) ON equipment TO cg_ui;
        GRANT DELETE ON objects, equipment, latest_state,
                        history, state_events, events,
                        gps_latest_filtered TO cg_ui;
    END IF;
END
$$;

-- cg_writer: запись телеметрии
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'cg_writer') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE
            ON objects, equipment, register_catalog,
               gps_raw_history, gps_latest_filtered,
               latest_state, history,
               state_events, parameter_history, events
            TO cg_writer;
        GRANT USAGE ON SEQUENCE
            state_events_id_seq, parameter_history_id_seq, events_id_seq
            TO cg_writer;
    END IF;
END
$$;
