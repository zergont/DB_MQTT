-- Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
-- Программный комплекс «Честная Генерация»
-- Модуль записи телеметрии в базу данных
-- Автор: Саввиди Александр Анатольевич | ИНН 4725009270
--
-- Конфиденциальная информация. Несанкционированное использование запрещено.

-- =============================================================================
-- CG Analytics — схема для сервера аналитики
--
-- Чистый PostgreSQL, TimescaleDB НЕ требуется.
-- Данные поступают через логическую репликацию из cg_telemetry.
--
-- Применить перед созданием подписки:
--   psql -U postgres -d cg_analytics -f schema/schema_analytics.sql
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Справочные таблицы
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
    manufacturer    TEXT,
    model           TEXT,
    engine_sn       TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (router_sn, equip_type, panel_id)
);

CREATE TABLE IF NOT EXISTS register_catalog (
    equip_type        TEXT        NOT NULL,
    addr              INT         NOT NULL,
    name_default      TEXT,
    unit_default      TEXT,
    register_kind     TEXT        NOT NULL DEFAULT 'analog'
                      CHECK (register_kind IN ('analog', 'enum', 'fault_bitmap')),
    value_kind        TEXT        NOT NULL DEFAULT 'analog',
    tolerance         NUMERIC,
    min_interval_sec  INT,
    heartbeat_sec     INT,
    store_history     BOOLEAN     NOT NULL DEFAULT true,
    states_json       JSONB,
    name_ru           TEXT,
    PRIMARY KEY (equip_type, addr)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. History — аналоговые данные
--
--    Обычная таблица без TimescaleDB. Данные накапливаются бессрочно —
--    retention управляется аналитикой самостоятельно.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS history (
    router_sn       TEXT        NOT NULL,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    addr            INT         NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    value           NUMERIC,
    raw             INT
);

CREATE INDEX IF NOT EXISTS idx_history_key_ts
    ON history (router_sn, equip_type, panel_id, addr, ts DESC);

CREATE INDEX IF NOT EXISTS idx_history_received_at
    ON history (received_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Parameter history — уставки и настройки оборудования
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
-- 4. Events — системные события (online/offline и др.)
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
-- 5. Data gaps — разрывы связи с оборудованием
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS data_gaps (
    id          BIGSERIAL   PRIMARY KEY,
    router_sn   TEXT        NOT NULL,
    equip_type  TEXT        NOT NULL,
    panel_id    INT         NOT NULL,
    gap_start   TIMESTAMPTZ NOT NULL,
    gap_end     TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_data_gaps_equip_time
    ON data_gaps (router_sn, equip_type, panel_id, gap_start DESC);

CREATE INDEX IF NOT EXISTS idx_data_gaps_open
    ON data_gaps (router_sn, equip_type, panel_id)
    WHERE gap_end IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Enum history — история дискретных состояний
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS enum_history (
    id          BIGSERIAL    PRIMARY KEY,
    router_sn   TEXT         NOT NULL,
    equip_type  TEXT         NOT NULL,
    panel_id    INT          NOT NULL,
    addr        INT          NOT NULL,
    value       INT          NOT NULL,
    state_start TIMESTAMPTZ  NOT NULL,
    state_end   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_enum_history_equip
    ON enum_history (router_sn, equip_type, panel_id, addr, state_start DESC);

CREATE INDEX IF NOT EXISTS idx_enum_history_open
    ON enum_history (router_sn, equip_type, panel_id, addr)
    WHERE state_end IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Fault history — история аварий (отдельные биты fault_bitmap)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fault_history (
    id          BIGSERIAL    PRIMARY KEY,
    router_sn   TEXT         NOT NULL,
    equip_type  TEXT         NOT NULL,
    panel_id    INT          NOT NULL,
    addr        INT          NOT NULL,
    bit         INT          NOT NULL,
    fault_start TIMESTAMPTZ  NOT NULL,
    fault_end   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fault_history_equip
    ON fault_history (router_sn, equip_type, panel_id, addr, fault_start DESC);

CREATE INDEX IF NOT EXISTS idx_fault_history_open
    ON fault_history (router_sn, equip_type, panel_id, addr)
    WHERE fault_end IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. Обогащённое представление history (history + register_catalog)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW history_rich AS
SELECT
    h.router_sn,
    h.equip_type,
    h.panel_id,
    h.addr,
    h.ts,
    h.received_at,
    h.value,
    h.raw,
    r.name_default  AS name,
    r.unit_default  AS unit,
    r.register_kind,
    r.states_json,
    r.name_ru
FROM history h
LEFT JOIN register_catalog r
    ON r.equip_type = h.equip_type
   AND r.addr       = h.addr;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. Права доступа
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'cg_analytics') THEN
        CREATE ROLE cg_analytics WITH LOGIN PASSWORD 'cg_analytics_pass';
        RAISE NOTICE 'Role cg_analytics created — смените пароль в production!';
    END IF;
END
$$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO cg_analytics;
