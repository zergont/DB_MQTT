-- =============================================================================
-- =============================================================================
-- CG DB-Writer: PostgreSQL schema
-- Применить:  psql -U <user> -d cg_telemetry -f schema/001_init.sql
-- =============================================================================

-- 1) Справочные таблицы ------------------------------------------------------

CREATE TABLE IF NOT EXISTS objects (
    router_sn   TEXT PRIMARY KEY,
    name        TEXT,
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS equipment (
    router_sn       TEXT        NOT NULL REFERENCES objects(router_sn) ON DELETE CASCADE,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (router_sn, equip_type, panel_id)
);

CREATE TABLE IF NOT EXISTS register_catalog (
    equip_type      TEXT        NOT NULL,
    addr            INT         NOT NULL,
    name_default    TEXT,
    unit_default    TEXT,
    value_kind      TEXT        NOT NULL DEFAULT 'analog',   -- analog|discrete|counter|enum|text
    tolerance       NUMERIC,
    min_interval_sec INT,
    heartbeat_sec   INT,
    store_history   BOOL        NOT NULL DEFAULT true,
    PRIMARY KEY (equip_type, addr)
);

-- 2) GPS данные объекта ------------------------------------------------------

CREATE TABLE IF NOT EXISTS gps_raw_history (
    id              BIGSERIAL PRIMARY KEY,
    router_sn       TEXT              NOT NULL,
    gps_time        TIMESTAMPTZ,
    received_at     TIMESTAMPTZ       NOT NULL DEFAULT now(),
    lat             DOUBLE PRECISION  NOT NULL,
    lon             DOUBLE PRECISION  NOT NULL,
    satellites      INT,
    fix_status      INT,
    accepted        BOOL              NOT NULL,
    reject_reason   TEXT
);

CREATE INDEX IF NOT EXISTS idx_gps_raw_sn_recv
    ON gps_raw_history (router_sn, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_gps_raw_sn_time
    ON gps_raw_history (router_sn, gps_time DESC);

CREATE TABLE IF NOT EXISTS gps_latest_filtered (
    router_sn   TEXT PRIMARY KEY,
    gps_time    TIMESTAMPTZ,
    received_at TIMESTAMPTZ       NOT NULL,
    lat         DOUBLE PRECISION  NOT NULL,
    lon         DOUBLE PRECISION  NOT NULL,
    satellites  INT,
    fix_status  INT
);

-- 3) Панели: latest + history ------------------------------------------------

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

CREATE TABLE IF NOT EXISTS history (
    id              BIGSERIAL PRIMARY KEY,
    router_sn       TEXT        NOT NULL,
    equip_type      TEXT        NOT NULL,
    panel_id        INT         NOT NULL,
    addr            INT         NOT NULL,
    ts              TIMESTAMPTZ,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    value           NUMERIC,
    raw             INT,
    text            TEXT,
    reason          TEXT,
    write_reason    TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_key_ts
    ON history (router_sn, equip_type, panel_id, addr, ts DESC);

-- 4) События -----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
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
