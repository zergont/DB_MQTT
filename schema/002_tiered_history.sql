-- =============================================================================
-- CG DB-Writer: Tiered history — агрегатные таблицы (1min, 1hour)
-- Применить:  psql -U <user> -d cg_telemetry -f schema/002_tiered_history.sql
-- =============================================================================

-- 1-минутные агрегаты --------------------------------------------------------

CREATE TABLE IF NOT EXISTS history_1min (
    router_sn    TEXT        NOT NULL,
    equip_type   TEXT        NOT NULL,
    panel_id     INT         NOT NULL,
    addr         INT         NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,  -- начало минуты (floor to minute)
    avg_value    NUMERIC,
    min_value    NUMERIC,
    max_value    NUMERIC,
    sample_count INT         NOT NULL,
    PRIMARY KEY (router_sn, equip_type, panel_id, addr, ts)
);

CREATE INDEX IF NOT EXISTS idx_history_1min_ts
    ON history_1min (ts);

-- 1-часовые агрегаты ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS history_1hour (
    router_sn    TEXT        NOT NULL,
    equip_type   TEXT        NOT NULL,
    panel_id     INT         NOT NULL,
    addr         INT         NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,  -- начало часа (floor to hour)
    avg_value    NUMERIC,
    min_value    NUMERIC,
    max_value    NUMERIC,
    sample_count INT         NOT NULL,
    PRIMARY KEY (router_sn, equip_type, panel_id, addr, ts)
);

CREATE INDEX IF NOT EXISTS idx_history_1hour_ts
    ON history_1hour (ts);

-- Права ----------------------------------------------------------------------

GRANT SELECT ON history_1min  TO cg_ui;
GRANT SELECT ON history_1hour TO cg_ui;

GRANT SELECT, INSERT, UPDATE, DELETE ON history_1min  TO cg_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON history_1hour TO cg_writer;
