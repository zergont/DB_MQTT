"""CG DB-Writer — маршруты веб-интерфейса."""

from __future__ import annotations

from pathlib import Path

from aiohttp import web

from src.web.config_api import (
    handle_config_download,
    handle_config_get,
    handle_config_put,
    handle_config_upload,
    handle_restart,
)


def setup_routes(app: web.Application, config_path: Path) -> None:
    """Зарегистрировать маршруты веб-интерфейса."""
    app["config_path"] = config_path

    static_dir = Path(__file__).parent / "static"

    # Главная страница
    app.router.add_get("/", _handle_index)

    # API конфигурации
    app.router.add_get("/api/config", handle_config_get)
    app.router.add_put("/api/config", handle_config_put)
    app.router.add_get("/api/config/download", handle_config_download)
    app.router.add_post("/api/config/upload", handle_config_upload)
    app.router.add_post("/api/restart", handle_restart)

    # Статика (CSS, JS)
    app.router.add_static("/static", static_dir, show_index=False)


async def _handle_index(request: web.Request) -> web.FileResponse:
    return web.FileResponse(Path(__file__).parent / "static" / "index.html")
