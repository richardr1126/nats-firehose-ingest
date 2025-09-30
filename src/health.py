from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .metrics import nats_connected


def create_health_api() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health():
        # Basic health check
        return {"status": "ok"}

    @app.get("/ready")
    async def ready():
        # Consider NATS connection as readiness condition
        return {"ready": nats_connected._value.get() == 1}

    @app.get("/metrics")
    async def metrics():
        data = generate_latest()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    return app
