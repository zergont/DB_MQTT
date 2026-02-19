"""
CG DB-Writer — настройка логирования.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.config import LoggingCfg


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(obj, ensure_ascii=False)


def setup_logging(cfg: LoggingCfg) -> None:
    level = getattr(logging, cfg.level.upper(), logging.INFO)

    handlers: list[logging.Handler] = []

    console = logging.StreamHandler(sys.stdout)
    handlers.append(console)

    if cfg.log_file:
        fh = logging.FileHandler(cfg.log_file, encoding="utf-8")
        handlers.append(fh)

    if cfg.json_logs:
        fmt = _JsonFormatter()
    else:
        fmt = logging.Formatter(
            "%(asctime)s  %(levelname)-7s  [%(name)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    root = logging.getLogger()
    root.setLevel(level)
    for h in handlers:
        h.setFormatter(fmt)
        root.addHandler(h)

    # Приглушаем шумные библиотеки
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiomqtt").setLevel(logging.WARNING)
