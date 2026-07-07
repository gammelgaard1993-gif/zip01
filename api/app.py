from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from typing import AsyncIterator

from fastapi import FastAPI

from api.routes.health import router as health_router
from api.routes.occupancy import router as occupancy_router
from api.routes.alarms import router as alarms_router
from api.routes.metrics import router as metrics_router
from api.routes.events import router as events_router
from ingestion.queue import PriorityEventQueue
from ingestion.mqtt_subscriber import MQTTSubscriber
from processing.alarm_bus import AlarmBus
from processing.worker_pool import WorkerPool
from core.recovery import RecoveryManager
from config import NORMAL_QUEUE_MAX_SIZE, ENABLE_MQTT, MQTT_BROKER_URL, MQTT_BROKER_PORT, MQTT_TOPIC

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    from core.db import init_db
    from core.redis_client import get_redis_client

    app.state.db_connection = init_db()
    app.state.redis_client = get_redis_client()
    app.state.alarm_bus = AlarmBus()
    app.state.event_queue = PriorityEventQueue(NORMAL_QUEUE_MAX_SIZE)
    app.state.worker_pool = WorkerPool(
        event_queue=app.state.event_queue,
        alarm_bus=app.state.alarm_bus,
        db_connection=app.state.db_connection,
        redis_client=app.state.redis_client,
    )
    app.state.mqtt_subscriber = None
    if ENABLE_MQTT:
        app.state.mqtt_subscriber = MQTTSubscriber(
            broker_url=MQTT_BROKER_URL,
            broker_port=MQTT_BROKER_PORT,
            topic=MQTT_TOPIC,
            event_queue=app.state.event_queue,
        )

    recovery_manager = RecoveryManager(
        db_connection=app.state.db_connection,
        redis_client=app.state.redis_client,
        alarm_bus=app.state.alarm_bus,
    )
    app.state.recovery_manager = recovery_manager
    await recovery_manager.restore_state()
    await recovery_manager.start_snapshot_loop()

    if app.state.mqtt_subscriber is not None:
        app.state.mqtt_subscriber.start()
    await app.state.worker_pool.start()
    try:
        yield
    finally:
        mqtt_subscriber = getattr(app.state, "mqtt_subscriber", None)
        if mqtt_subscriber is not None:
            mqtt_subscriber.stop()

        worker_pool = getattr(app.state, "worker_pool", None)
        if worker_pool is not None:
            await worker_pool.stop()

        recovery_manager = getattr(app.state, "recovery_manager", None)
        if recovery_manager is not None:
            await recovery_manager.stop_snapshot_loop()

        db_connection = getattr(app.state, "db_connection", None)
        if db_connection is not None:
            db_connection.close()


app = FastAPI(title="Teton Backend", lifespan=lifespan)
app.include_router(health_router)
app.include_router(occupancy_router)
app.include_router(alarms_router)
app.include_router(metrics_router)
app.include_router(events_router)
