-- Copyright (c) 2026 ООО «НГ-ЭНЕРГОСЕРВИС». Все права защищены.
-- Программный комплекс «Честная Генерация»
-- Модуль записи телеметрии в базу данных
-- Автор: Саввиди Александр Анатольевич | ИНН 4725009270
--
-- Конфиденциальная информация. Несанкционированное использование запрещено.

-- =============================================================================
-- CG DB-Writer 2.9.0 — миграция history: один ряд на (регистр, ts), ts_status
--
-- Запуск на сервере БД вручную (update.sh его не применяет):
--   sudo -u postgres psql -d cg_telemetry -v ON_ERROR_STOP=1 \
--        -f /opt/db-writer/schema/migrate_2.9.0.sql
-- Без столбца ts_status (только дубли и индекс):
--   ... -v skip_ts_status=1 -f ...
--
-- Порядок:
--   0. Если history реплицируется на сервер аналитики — сначала там:
--        ALTER TABLE history ADD COLUMN IF NOT EXISTS ts_status TEXT;
--      (есть в schema_analytics.sql). Иначе подписка встанет на незнакомом
--      столбце.
--   1. Этот скрипт. db-writer может работать: на время построения индекса
--      вставки ждут, очередь decoded их выдерживает.
--   2. Перезапуск db-writer — при старте он увидит столбец ts_status
--      (до этого пишет без него, WARNING в логе).
--
-- Повторный запуск безопасен. До миграции db-writer 2.9.0 тоже работает:
-- ON CONFLICT DO NOTHING без уникального индекса ничего не отсекает.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Версия TimescaleDB: ≥ 2.11 — INSERT ... ON CONFLICT / RETURNING в сжатый
--    чанк (поздняя запись старше 7 дней). Иначе — стоп, ничего не меняем.
-- ─────────────────────────────────────────────────────────────────────────────

\echo '== TimescaleDB'
DO $$
DECLARE
    v text := (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb');
    m text[] := regexp_match(v, '^(\d+)\.(\d+)');
BEGIN
    RAISE NOTICE 'timescaledb %', v;
    IF m IS NULL OR ARRAY[m[1]::int, m[2]::int] < ARRAY[2, 11] THEN
        RAISE EXCEPTION 'TimescaleDB % < 2.11: migration stopped, nothing changed', v;
    END IF;
END $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Дубли (регистр, ts)
-- ─────────────────────────────────────────────────────────────────────────────

\echo '== Дубли (регистр, ts)'
DROP TABLE IF EXISTS pg_temp.dup_keys;
CREATE TEMP TABLE dup_keys AS
SELECT router_sn, equip_type, panel_id, addr, ts, count(*) AS n
FROM history
GROUP BY router_sn, equip_type, panel_id, addr, ts
HAVING count(*) > 1;

SELECT count(*)                  AS groups,
       coalesce(sum(n - 1), 0)   AS rows_to_delete,
       min(ts)                   AS first_ts,
       max(ts)                   AS last_ts
FROM dup_keys;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Сжатые чанки с дублями — распаковать. ctid в сжатом чанке недоступен,
--    а уникальный индекс не проверяет уже сжатые строки. Политика сжатия
--    (compress_after 7 days) упакует их обратно сама.
-- ─────────────────────────────────────────────────────────────────────────────

\echo '== Распаковка сжатых чанков с дублями'
SELECT decompress_chunk(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass,
                        if_compressed => true) AS decompressed
FROM timescaledb_information.chunks c
WHERE c.hypertable_name = 'history'
  AND c.is_compressed
  AND EXISTS (SELECT 1 FROM dup_keys d WHERE d.ts >= c.range_start AND d.ts < c.range_end);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Удалить дубли по чанкам: остаётся строка с меньшим |received_at − ts|
--    (при равенстве — пришедшая раньше). Ключ целиком в одном чанке: чанк
--    режется по ts.
-- ─────────────────────────────────────────────────────────────────────────────

\echo '== Удаление дублей'
DO $$
DECLARE
    c record;
    n bigint;
    total bigint := 0;
BEGIN
    FOR c IN
        SELECT format('%I.%I', ch.chunk_schema, ch.chunk_name) AS rel
        FROM timescaledb_information.chunks ch
        WHERE ch.hypertable_name = 'history'
          AND EXISTS (SELECT 1 FROM dup_keys d
                      WHERE d.ts >= ch.range_start AND d.ts < ch.range_end)
    LOOP
        EXECUTE format($q$
            DELETE FROM %1$s h
            USING (
                SELECT x.ctid AS tid,
                       row_number() OVER (
                           PARTITION BY x.router_sn, x.equip_type, x.panel_id, x.addr, x.ts
                           ORDER BY abs(extract(epoch FROM x.received_at - x.ts)), x.received_at
                       ) AS rn
                FROM %1$s x
                JOIN dup_keys d USING (router_sn, equip_type, panel_id, addr, ts)
            ) r
            WHERE h.ctid = r.tid AND r.rn > 1
        $q$, c.rel);
        GET DIAGNOSTICS n = ROW_COUNT;
        total := total + n;
        RAISE NOTICE '%: % row(s) deleted', c.rel, n;
    END LOOP;
    RAISE NOTICE 'duplicates deleted: %', total;
END $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Уникальный индекс вместо idx_history_key_ts (те же столбцы). На время
--    построения вставки ждут. CONCURRENTLY на hypertable не поддерживается.
-- ─────────────────────────────────────────────────────────────────────────────

\echo '== Уникальный индекс'
CREATE UNIQUE INDEX IF NOT EXISTS idx_history_key_ts_uniq
    ON history (router_sn, equip_type, panel_id, addr, ts DESC);
DROP INDEX IF EXISTS idx_history_key_ts;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Столбец ts_status (на сервере аналитики — раньше, см. шаг 0)
-- ─────────────────────────────────────────────────────────────────────────────

\if :{?skip_ts_status}
\echo '== ts_status: пропущено (skip_ts_status)'
\else
\echo '== Столбец ts_status'
ALTER TABLE history ADD COLUMN IF NOT EXISTS ts_status TEXT;
\endif


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Проверка
-- ─────────────────────────────────────────────────────────────────────────────

\echo '== Проверка'
SELECT indexname, indexdef FROM pg_indexes
WHERE tablename = 'history' AND indexname LIKE 'idx_history_key_ts%';

SELECT column_name FROM information_schema.columns
WHERE table_name = 'history' AND column_name = 'ts_status';
