# Changelog

Все заметные изменения в проекте фиксируются в этом файле.

## [1.1.0] - 2026-07-14

### Added
- многоуровневый ретеншн: таблицы `history_1min`, `history_1hour` для агрегатов
- фоновая задача агрегации (`src/aggregation.py`): raw → 1min → 1hour
- dirty-tracking для переагрегации опоздавших записей
- watermark-защита: retention не удаляет raw данные до их агрегации
- weighted average для часовых агрегатов (`SUM(avg*count)/SUM(count)`)
- per-register настройки через `kpi_registers`: `equip_type`, `min_interval_sec`
- SQL-миграция `schema/002_tiered_history.sql`

### Changed
- `min_interval_sec` по умолчанию: 10 → 2 сек
- retention: единый `history_days=30` → раздельный `history_raw_days=7` / `history_1min_days=30` / `history_1hour_days=365`
- `batch_size` по умолчанию: 5000 → 10000
- `kpi_map()` ключ: `addr` → `(equip_type, addr)`

## [1.0.0] - 2026-03-09

### Added
- HTTP `health` endpoint с состояниями `ok/degraded/dead`
- вывод версии сервиса в лог старта и `/health`
- `scripts/update.sh` для обновления установленного сервиса

### Changed
- установка сервиса переведена на `/opt/db-writer`
- рабочий конфиг перенесён в `/etc/db-writer/config.yml`
- усилено завершение процесса при падении критического task
- обновлены параметры burst-нагрузки в `config.example.yml`
