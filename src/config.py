""" CG DB-Writer — загрузка и валидация конфигурации из YAML.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("cg.config")


# ---------------------------------------------------------------------------
# Data-classes
# ---------------------------------------------------------------------------

@dataclass
class MqttCfg:
    host: str = "localhost"
    port: int = 1883
    user: str = ""
    password: str = ""
    tls: bool = False
    client_id: str = "cg-db-writer"
    keepalive: int = 60
    reconnect_min_delay: int = 1
    reconnect_max_delay: int = 60

    sub_decoded: str = "cg/v1/decoded/SN/+/pcc/+"
    sub_telemetry: str = "cg/v1/telemetry/SN/+"

@dataclass
class PostgresCfg:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "cg_telemetry"
    user: str = ""
    password: str = ""

    pool_min: int = 2
    pool_max: int = 10

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )

@dataclass
class IngestCfg:
    """Настройки приёма сообщений и защиты от перегрузки.

    Идея: MQTT loop максимально быстро читает сообщения и складывает в очереди,
    а воркеры пишут в БД. Это даёт буфер и предсказуемость при лаге PostgreSQL.

    decoded_queue_maxsize:
        Очередь decoded сообщений (самый большой поток).
    telemetry_queue_maxsize:
        Очередь GPS/telemetry (маленький поток, но важный).
    worker_count:
        Кол-во DB-воркеров. Для стабильности по умолчанию 1 (без гонок по кэшам).
        Можно увеличить до 2-4 при достаточном pool_max и CPU.
    drop_decoded_when_full:
        Если decoded очередь переполнена — выбрасывать данные вместо блокировки MQTT loop.
        Для надёжности "в реальном времени" обычно лучше drop_oldest, чем зависание.
    """
    decoded_queue_maxsize: int = 5000
    telemetry_queue_maxsize: int = 200
    worker_count: int = 1

    # При переполнении decoded очереди:
    drop_decoded_when_full: bool = True
    drop_decoded_policy: str = "drop_oldest"  # drop_oldest | drop_new

    # Ретрай DB операций (внутри воркера)
    worker_max_retries: int = 3
    worker_retry_delay_sec: float = 2.0

@dataclass
class GpsFilterCfg:
    sats_min: int = 4
    fix_min: int = 1
    deadband_m: float = 30.0
    max_jump_m: float = 500.0
    max_speed_kmh: float = 120.0
    confirm_points: int = 3
    confirm_radius_m: float = 50.0

@dataclass
class KpiRegister:
    addr: int
    heartbeat_sec: int = 60
    tolerance: float = 0.1

@dataclass
class HistoryDefaults:
    tolerance_analog: float = 0.5
    min_interval_sec: int = 10
    heartbeat_sec: int = 900
    store_history: bool = True
    value_kind: str = "analog"

@dataclass
class HistoryPolicyCfg:
    defaults: HistoryDefaults = field(default_factory=HistoryDefaults)
    kpi_registers: list[KpiRegister] = field(default_factory=list)

    def kpi_map(self) -> dict[int, KpiRegister]:
        """addr → KpiRegister для быстрого поиска."""
        return {k.addr: k for k in self.kpi_registers}

@dataclass
class EventsPolicyCfg:
    router_stale_sec: int = 120
    router_offline_sec: int = 300
    panel_stale_sec: int = 120
    panel_offline_sec: int = 300
    check_interval_sec: int = 30

    enable_gps_reject_events: bool = True
    enable_unknown_register_events: bool = True

@dataclass
class RetentionCfg:
    gps_raw_hours: int = 72
    history_days: int = 30
    events_days: int = 90

    cleanup_interval_hours: int = 24
    batch_size: int = 5000

@dataclass
class LoggingCfg:
    level: str = "INFO"
    log_file: str = ""
    json_logs: bool = False

@dataclass
class AppConfig:
    mqtt: MqttCfg = field(default_factory=MqttCfg)
    postgres: PostgresCfg = field(default_factory=PostgresCfg)
    ingest: IngestCfg = field(default_factory=IngestCfg)
    gps_filter: GpsFilterCfg = field(default_factory=GpsFilterCfg)
    history_policy: HistoryPolicyCfg = field(default_factory=HistoryPolicyCfg)
    events_policy: EventsPolicyCfg = field(default_factory=EventsPolicyCfg)
    retention: RetentionCfg = field(default_factory=RetentionCfg)
    logging: LoggingCfg = field(default_factory=LoggingCfg)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _merge(dc_class: type, raw: dict[str, Any] | None):
    """Создаёт dataclass из dict, игнорируя лишние ключи."""
    if raw is None:
        return dc_class()
    known = {f.name for f in dc_class.__dataclass_fields__.values()}
    return dc_class(**{k: v for k, v in raw.items() if k in known})


def _parse_mqtt(raw: dict[str, Any] | None) -> MqttCfg:
    if raw is None:
        return MqttCfg()
    subs = raw.pop("subscriptions", {}) or {}
    cfg = _merge(MqttCfg, raw)
    if "decoded" in subs:
        cfg.sub_decoded = subs["decoded"]
    if "telemetry" in subs:
        cfg.sub_telemetry = subs["telemetry"]
    return cfg


def _parse_history(raw: dict[str, Any] | None) -> HistoryPolicyCfg:
    if raw is None:
        return HistoryPolicyCfg()
    defaults = _merge(HistoryDefaults, raw.get("defaults"))
    kpi_raw = raw.get("kpi_registers") or []
    kpis = [_merge(KpiRegister, k) for k in kpi_raw]
    return HistoryPolicyCfg(defaults=defaults, kpi_registers=kpis)


def load_config(path: str | Path) -> AppConfig:
    """Загрузить конфигурацию из YAML файла."""
    p = Path(path)
    if not p.exists():
        logger.error("Config file not found: %s", p)
        sys.exit(1)

    with p.open("r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    cfg = AppConfig(
        mqtt=_parse_mqtt(raw.get("mqtt")),
        postgres=_merge(PostgresCfg, raw.get("postgres")),
        ingest=_merge(IngestCfg, raw.get("ingest")),
        gps_filter=_merge(GpsFilterCfg, raw.get("gps_filter")),
        history_policy=_parse_history(raw.get("history_policy")),
        events_policy=_merge(EventsPolicyCfg, raw.get("events_policy")),
        retention=_merge(RetentionCfg, raw.get("retention")),
        logging=_merge(LoggingCfg, raw.get("logging")),
    )
    logger.info("Config loaded from %s", p)
    return cfg
