"""
CG DB-Writer — главная точка входа.

Использование:
    python -m src.main --config config.yml
    python -m src.main --config config.yml --cleanup   (только очистка, без MQTT)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

import aiomqtt

from src import db
from src.config import AppConfig, load_config
from src.gps_filter import GpsPoint
from src.handlers import dispatch, get_gps_filter
from src.log import setup_logging
from src.retention import retention_loop, _do_cleanup
from src.watchdog import watchdog_loop

logger = logging.getLogger("cg.main")

# Shared state
_last_seen: dict[str, datetime] = {}
_panel_last_seen: dict[tuple[str, int], datetime] = {}


async def _restore_gps_state(cfg: AppConfig) -> None:
    """При старте загружаем последние принятые GPS точки в фильтры."""
    async with db.pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM gps_latest_filtered")
    for r in rows:
        flt = get_gps_filter(r["router_sn"], cfg)
        flt.set_initial(GpsPoint(
            lat=r["lat"], lon=r["lon"],
            satellites=r["satellites"], fix_status=r["fix_status"],
            gps_time=r["gps_time"],
            received_at=r["received_at"],
        ))
    logger.info("Restored GPS state for %d objects", len(rows))


async def _mqtt_loop(cfg: AppConfig) -> None:
    """Подключение к MQTT с автореконнектом."""
    mc = cfg.mqtt
    delay = mc.reconnect_min_delay

    while True:
        try:
            logger.info("Connecting to MQTT %s:%d …", mc.host, mc.port)
            async with aiomqtt.Client(
                hostname=mc.host,
                port=mc.port,
                username=mc.user or None,
                password=mc.password or None,
                identifier=mc.client_id,
                keepalive=mc.keepalive,
                tls_params=aiomqtt.TLSParameters() if mc.tls else None,
            ) as client:
                await client.subscribe(mc.sub_decoded)
                await client.subscribe(mc.sub_telemetry)
                logger.info(
                    "MQTT connected, subscribed: %s, %s",
                    mc.sub_decoded, mc.sub_telemetry,
                )
                delay = mc.reconnect_min_delay  # сброс задержки при успехе

                async for msg in client.messages:
                    topic = str(msg.topic)
                    payload = msg.payload
                    if not isinstance(payload, bytes):
                        payload = str(payload).encode() if payload else b""
                    try:
                        await dispatch(
                            topic,
                            payload,
                            cfg,
                            _last_seen,
                            _panel_last_seen,
                        )
                    except Exception:
                        logger.exception("Error processing %s", topic)

        except aiomqtt.MqttError as e:
            logger.warning("MQTT connection lost: %s  (reconnect in %ds)", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, mc.reconnect_max_delay)
        except asyncio.CancelledError:
            logger.info("MQTT loop cancelled")
            return


async def _run(cfg: AppConfig) -> None:
    """Запускает все подсистемы."""
    pool = await db.init_pool(cfg.postgres)
    try:
        await _restore_gps_state(cfg)

        tasks = [
            asyncio.create_task(_mqtt_loop(cfg), name="mqtt"),
            asyncio.create_task(
                watchdog_loop(cfg, _last_seen, _panel_last_seen),
                name="watchdog",
            ),
            asyncio.create_task(
                retention_loop(cfg.retention),
                name="retention",
            ),
        ]

        # Ждём завершения любого — при штатной остановке все отменятся
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_EXCEPTION,
        )
        for t in done:
            if t.exception():
                logger.error("Task %s failed: %s", t.get_name(), t.exception())
        for t in pending:
            t.cancel()

    finally:
        await db.close_pool()


async def _run_cleanup(cfg: AppConfig) -> None:
    """Однократная очистка (CLI)."""
    await db.init_pool(cfg.postgres)
    try:
        await _do_cleanup(cfg.retention)
    finally:
        await db.close_pool()


def _shutdown(loop: asyncio.AbstractEventLoop) -> None:
    logger.info("Shutdown signal received")
    for task in asyncio.all_tasks(loop):
        task.cancel()


def main() -> None:
    parser = argparse.ArgumentParser(description="CG DB-Writer")
    parser.add_argument(
        "-c", "--config",
        default="config.yml",
        help="Path to config.yml (default: config.yml)",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Run retention cleanup once and exit",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.logging)

    if args.cleanup:
        asyncio.run(_run_cleanup(cfg))
    else:
        loop = asyncio.new_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _shutdown, loop)
            except NotImplementedError:
                # Windows не поддерживает add_signal_handler
                pass
        try:
            loop.run_until_complete(_run(cfg))
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Shutting down…")
        finally:
            loop.close()


if __name__ == "__main__":
    main()
