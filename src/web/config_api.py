"""CG DB-Writer — API для работы с конфигурацией."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from aiohttp import web

from src.config import config_to_dict, parse_config_dict

logger = logging.getLogger("cg.web")


def _load_raw(config_path: Path) -> dict:
    """Прочитать сырой YAML с диска."""
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _save_raw(config_path: Path, raw: dict) -> None:
    """Сохранить dict в YAML, предварительно создав .bak."""
    if config_path.exists():
        bak = config_path.with_suffix(".yml.bak")
        shutil.copy2(config_path, bak)

    with config_path.open("w", encoding="utf-8") as f:
        yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/config — текущий конфиг как JSON
# ─────────────────────────────────────────────────────────────────────────────

async def handle_config_get(request: web.Request) -> web.Response:
    config_path: Path = request.app["config_path"]
    try:
        raw = _load_raw(config_path)
        # Валидируем, чтобы вернуть нормализованный вид
        cfg = parse_config_dict(raw)
        result = config_to_dict(cfg)
        # Добавляем config_version из файла (может отсутствовать)
        result["config_version"] = raw.get("config_version", 0)
        return web.json_response(result)
    except Exception as e:
        logger.exception("Failed to read config")
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# PUT /api/config — сохранить конфиг
# ─────────────────────────────────────────────────────────────────────────────

async def handle_config_put(request: web.Request) -> web.Response:
    config_path: Path = request.app["config_path"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Невалидный JSON"}, status=400)

    # Извлекаем config_version из запроса
    new_version = body.pop("config_version", 0)

    try:
        # Валидация: пробуем собрать AppConfig
        parse_config_dict(body)
    except Exception as e:
        return web.json_response({"error": f"Ошибка валидации: {e}"}, status=400)

    try:
        # Читаем текущую версию
        current_raw = _load_raw(config_path)
        current_version = current_raw.get("config_version", 0)

        # Инкрементируем версию
        body["config_version"] = max(current_version, new_version) + 1

        _save_raw(config_path, body)
        logger.info("Config saved (version %d → %d)", current_version, body["config_version"])

        return web.json_response({
            "ok": True,
            "config_version": body["config_version"],
            "restart_required": True,
        })
    except Exception as e:
        logger.exception("Failed to save config")
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/config/download — скачать config.yml
# ─────────────────────────────────────────────────────────────────────────────

async def handle_config_download(request: web.Request) -> web.Response:
    config_path: Path = request.app["config_path"]
    if not config_path.exists():
        return web.json_response({"error": "Файл не найден"}, status=404)

    return web.FileResponse(
        config_path,
        headers={
            "Content-Disposition": f'attachment; filename="config.yml"',
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/config/upload — загрузить config.yml (восстановление)
# ─────────────────────────────────────────────────────────────────────────────

async def handle_config_upload(request: web.Request) -> web.Response:
    config_path: Path = request.app["config_path"]
    try:
        reader = await request.multipart()
        field = await reader.next()
        if field is None:
            return web.json_response({"error": "Файл не найден в запросе"}, status=400)

        content = await field.read(decode=True)
        raw = yaml.safe_load(content)
        if not isinstance(raw, dict):
            return web.json_response({"error": "Невалидный YAML"}, status=400)

        # Валидация
        version_in_file = raw.get("config_version", 0)
        raw_no_ver = {k: v for k, v in raw.items() if k != "config_version"}
        parse_config_dict(raw_no_ver)

        # Сохраняем с инкрементом версии
        current_raw = _load_raw(config_path) if config_path.exists() else {}
        current_version = current_raw.get("config_version", 0)
        raw["config_version"] = max(current_version, version_in_file) + 1

        _save_raw(config_path, raw)
        logger.info("Config uploaded (version → %d)", raw["config_version"])

        return web.json_response({
            "ok": True,
            "config_version": raw["config_version"],
            "restart_required": True,
        })
    except web.HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to upload config")
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────────────────
# POST /api/restart — перезапуск сервиса
# ─────────────────────────────────────────────────────────────────────────────

async def handle_restart(request: web.Request) -> web.Response:
    """Перезапуск процесса через os.execv (Unix) или sys.exit + systemd (Windows)."""
    logger.info("Restart requested via web UI")

    # Отправляем ответ до перезапуска
    response = web.json_response({"ok": True, "message": "Перезапуск..."})
    await response.prepare(request)
    await response.write_eof()

    # На Unix — os.execv заменяет текущий процесс
    # На Windows или под systemd — sys.exit(0), systemd перезапустит (Restart=on-failure)
    if sys.platform != "win32":
        try:
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except OSError:
            pass

    # Fallback: просто завершаем (systemd / supervisor перезапустит)
    os._exit(0)
