"""CG DB-Writer — API управления оборудованием."""

from __future__ import annotations

import logging

from aiohttp import web

from src import db

logger = logging.getLogger("cg.web.equipment")


async def handle_equipment_get(request: web.Request) -> web.Response:
    """GET /api/equipment — список всего оборудования."""
    try:
        async with db.pool().acquire() as conn:
            rows = await db.get_all_equipment(conn)
        result = [
            {
                "router_sn":    r["router_sn"],
                "equip_type":   r["equip_type"],
                "panel_id":     r["panel_id"],
                "name":         r["name"] or "",
                "manufacturer": r["manufacturer"] or "",
                "model":        r["model"] or "",
                "engine_sn":    r["engine_sn"] or "",
                "object_name":  r["object_name"] or "",
                "last_seen_at": r["last_seen_at"].isoformat() if r["last_seen_at"] else None,
            }
            for r in rows
        ]
        return web.json_response(result)
    except Exception as e:
        logger.error("equipment_get error: %s", e)
        return web.json_response({"error": str(e)}, status=500)


async def handle_equipment_put(request: web.Request) -> web.Response:
    """PUT /api/equipment — сохранить метаданные оборудования."""
    try:
        items = await request.json()
        if not isinstance(items, list):
            return web.json_response({"error": "expected list"}, status=400)

        async with db.pool().acquire() as conn:
            for item in items:
                await db.update_equipment_meta(
                    conn,
                    router_sn=item["router_sn"],
                    equip_type=item["equip_type"],
                    panel_id=int(item["panel_id"]),
                    name=item.get("name") or None,
                    manufacturer=item.get("manufacturer") or None,
                    model=item.get("model") or None,
                    engine_sn=item.get("engine_sn") or None,
                )
        return web.json_response({"ok": True})
    except Exception as e:
        logger.error("equipment_put error: %s", e)
        return web.json_response({"error": str(e)}, status=500)
