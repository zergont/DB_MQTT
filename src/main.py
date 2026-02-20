""" CG DB-Writer — главная точка входа.

Использование:
  python -m src.main --config config.yml
  python -m src.main --config config.yml --cleanup   (только очистка, без MQTT)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from dataclasses import dataclass
from datetime import datetime, timezone

import aiomqtt

from src import db
from src.config import AppConfig, load_config
from src.gps_filter import GpsPoint
from src.handlers import dispatch, get_gps_filter
from src.log import setup_logging
from src.retention import _do_cleanup, retention_loop
from src.watchdog import watchdog_loop

logger = logging.getLogger("cg.main")

# Shared state for watchdog
_last_seen: dict[str, datetime] = {}
_panel_last_seen: dict[tuple[str, int], datetime] = {}


@dataclass(slots=True)
class _IngestItem:
    topic: str
    payload: bytes
    received_at: datetime
    kind: str  # "telemetry" | "decoded"


def _touch_last_seen(topic: str) -> None:
    """Обновить last_seen на момент получения сообщения (не на момент записи в БД).

    Это важно: если БД временно тормозит, watchdog не должен считать объект offline,
    если сообщения продолжают приходить.
    """
    now = datetime.now(timezone.utc)
    parts = topic.split("/")
    # telemetry: cg/v1/telemetry/SN/<sn>
    if len(parts) == 5 and parts[0] == "cg" and parts[2] == "telemetry" and parts[3] == "SN":
        sn = parts[4]
        _last_seen[sn] = now
        return

    # decoded: cg/v1/decoded/SN/<sn>/pcc/<panel_id>
    if len(parts) == 7 and parts[0] == "cg" and parts[2] == "decoded" and parts[3] == "SN" and parts[5] == "pcc":
        sn = parts[4]
        try:
            panel_id = int(parts[6])
        except ValueError:
            panel_id = None
        _last_seen[sn] = now
        if panel_id is not None:
            _panel_last_seen[(sn, panel_id)] = now


async def _restore_gps_state(cfg: AppConfig) -> None:
    """При старте загружаем последние принятые GPS точки в фильтры."""
    async with db.pool().acquire() as conn:
        rows = await conn.fetch("SELECT * FROM gps_latest_filtered")
        for r in rows:
            flt = get_gps_filter(r["router_sn"], cfg)
            flt.set_initial(
                GpsPoint(
                    lat=r["lat"],
                    lon=r["lon"],
                    satellites=r["satellites"],
                    fix_status=r["fix_status"],
                    gps_time=r["gps_time"],
                    received_at=r["received_at"],
                )
            )
        logger.info("Restored GPS state for %d objects", len(rows))


async def _queue_put(
    q: asyncio.Queue[_IngestItem],
    item: _IngestItem,
    *,
    drop_when_full: bool,
    drop_policy: str,
    log_name: str,
) -> None:
    """Положить в очередь. При переполнении — по политике drop."""
    try:
        q.put_nowait(item)
        return
    except asyncio.QueueFull:
        if not drop_when_full:
            # блокируемся, т.е. делаем backpressure
            logger.warning("Queue %s full; blocking put (size=%d)", log_name, q.qsize())
            await q.put(item)
            return

        if drop_policy == "drop_new":
            logger.warning("Queue %s full; dropped NEW message topic=%s", log_name, item.topic)
            return

        # drop_oldest (по умолчанию)
        try:
            _ = q.get_nowait()
            q.task_done()
        except asyncio.QueueEmpty:
            pass
        try:
            q.put_nowait(item)
            logger.warning("Queue %s full; dropped OLDEST and enqueued NEW topic=%s", log_name, item.topic)
        except asyncio.QueueFull:
            logger.warning("Queue %s still full after drop; dropped NEW topic=%s", log_name, item.topic)


async def _mqtt_ingest_loop(
    cfg: AppConfig,
    q_telemetry: asyncio.Queue[_IngestItem],
    q_decoded: asyncio.Queue[_IngestItem],
) -> None:
    """Подключение к MQTT и запись сообщений в очереди (без тяжёлой логики)."""
    mc = cfg.mqtt
    ic = cfg.ingest

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
                    mc.sub_decoded,
                    mc.sub_telemetry,
                )
                delay = mc.reconnect_min_delay

                async for msg in client.messages:
                    topic = str(msg.topic)
                    payload = msg.payload
                    if not isinstance(payload, bytes):
                        payload = str(payload).encode()
                    if not payload:
                        payload = b""

                    _touch_last_seen(topic)

                    kind = "telemetry" if topic.startswith("cg/v1/telemetry/") else "decoded"
                    item = _IngestItem(
                        topic=topic,
                        payload=payload,
                        received_at=datetime.now(timezone.utc),
                        kind=kind,
                    )

                    if kind == "telemetry":
                        # telemetry важнее; очередь маленькая, переполняться не должна
                        await _queue_put(
                            q_telemetry,
                            item,
                            drop_when_full=False,
                            drop_policy="drop_oldest",
                            log_name="telemetry",
                        )
                    else:
                        await _queue_put(
                            q_decoded,
                            item,
                            drop_when_full=ic.drop_decoded_when_full,
                            drop_policy=ic.drop_decoded_policy,
                            log_name="decoded",
                        )

        except aiomqtt.MqttError as e:
            logger.warning("MQTT connection lost: %s (reconnect in %ds)", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, mc.reconnect_max_delay)
        except asyncio.CancelledError:
            logger.info("MQTT ingest loop cancelled")
            return


async def _worker_loop(
    worker_id: int,
    cfg: AppConfig,
    q_telemetry: asyncio.Queue[_IngestItem],
    q_decoded: asyncio.Queue[_IngestItem],
) -> None:
    """DB-воркер: вытаскивает из очередей и пишет в БД.

    Приоритет: telemetry → decoded.
    Ретрай: только если dispatch падает исключением (обычно временная ошибка БД).
    """
    ic = cfg.ingest
    logger.info(
        "Worker-%d started (retries=%d, delay=%.1fs)",
        worker_id,
        ic.worker_max_retries,
        ic.worker_retry_delay_sec,
    )

    while True:
        # telemetry always first (важно для GPS и router_last_seen)
        src = "telemetry"
        try:
            item = q_telemetry.get_nowait()
        except asyncio.QueueEmpty:
            src = "decoded"
            item = await q_decoded.get()

        try:
            attempt = 0
            while True:
                try:
                    await dispatch(item.topic, item.payload, cfg, _last_seen, _panel_last_seen)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    attempt += 1
                    if attempt > ic.worker_max_retries:
                        logger.exception(
                            "Worker-%d: failed processing topic=%s after %d retries",
                            worker_id,
                            item.topic,
                            ic.worker_max_retries,
                        )
                        break
                    logger.warning(
                        "Worker-%d: error on topic=%s (%s); retry in %.1fs (%d/%d)",
                        worker_id,
                        item.topic,
                        type(e).__name__,
                        ic.worker_retry_delay_sec,
                        attempt,
                        ic.worker_max_retries,
                    )
                    await asyncio.sleep(ic.worker_retry_delay_sec)

        finally:
            if src == "telemetry":
                q_telemetry.task_done()
            else:
                q_decoded.task_done()


async def _run(cfg: AppConfig) -> None:
    """Запускает все подсистемы."""
    await db.init_pool(cfg.postgres)

    # Очереди
    q_telemetry: asyncio.Queue[_IngestItem] = asyncio.Queue(maxsize=cfg.ingest.telemetry_queue_maxsize)
    q_decoded: asyncio.Queue[_IngestItem] = asyncio.Queue(maxsize=cfg.ingest.decoded_queue_maxsize)

    try:
        await _restore_gps_state(cfg)

        tasks: list[asyncio.Task] = [
            asyncio.create_task(_mqtt_ingest_loop(cfg, q_telemetry, q_decoded), name="mqtt_ingest"),
            asyncio.create_task(watchdog_loop(cfg, _last_seen, _panel_last_seen), name="watchdog"),
            asyncio.create_task(retention_loop(cfg.retention), name="retention"),
        ]

        # Воркеры (DB writers)
        for i in range(max(1, int(cfg.ingest.worker_count))):
            tasks.append(asyncio.create_task(_worker_loop(i + 1, cfg, q_telemetry, q_decoded), name=f"worker_{i+1}"))

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
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
        "-c",
        "--config",
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
        return

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
