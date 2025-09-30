from __future__ import annotations

import asyncio
from typing import Optional

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.kv import KeyValue
from nats.js.api import KeyValueConfig, StreamConfig, RetentionPolicy, DiscardPolicy, StorageType
from nats.errors import TimeoutError

from .logging_setup import get_logger
from .metrics import nats_connected, posts_published_total, publish_errors_total
from .config import settings


log = get_logger(__name__)


class NatsService:
    def __init__(self, url: str, stream: str, subject: str, kv_bucket: str, max_retries: int = 3):
        self.url = url
        self.stream = stream
        self.subject = subject
        self.kv_bucket = kv_bucket
        self.max_retries = max_retries
        self.stream_num_replicas = settings.NUM_STREAM_REPLICAS

        self.nc: Optional[NATS] = None
        self.js: Optional[JetStreamContext] = None
        self.kv: Optional[KeyValue] = None

    async def connect(self):
        self.nc = NATS()
        await self.nc.connect(servers=[self.url])
        nats_connected.set(1)
        self.js = self.nc.jetstream()

        # Ensure stream exists
        try:
            await self.js.stream_info(self.stream)
            log.info("stream_exists", stream=self.stream)
        except Exception as e:
            log.info("stream_not_found", stream=self.stream, error=str(e))
            stream_config = StreamConfig(
                name=self.stream,
                subjects=[f"{self.subject}.>"],
                retention=RetentionPolicy.LIMITS,
                discard=DiscardPolicy.OLD,
                max_msgs_per_subject=-1,
                max_msgs=-1,
                max_bytes=-1,
                max_age=0,
                storage=StorageType.FILE,
                num_replicas=self.stream_num_replicas,
            )
            await self.js.add_stream(config=stream_config)
            log.info("stream_created", stream=self.stream)

        # Ensure KV exists
        try:
            self.kv = await self.js.key_value(self.kv_bucket)
            log.info("kv_exists", bucket=self.kv_bucket)
        except Exception as e:
            log.info("kv_not_found", bucket=self.kv_bucket, error=str(e))
            kv_config = KeyValueConfig(
                bucket=self.kv_bucket,
                history=1,
                storage=StorageType.FILE,
                replicas=self.stream_num_replicas,
            )
            self.kv = await self.js.create_key_value(config=kv_config)
            log.info("kv_created", bucket=self.kv_bucket)

    async def close(self):
        try:
            nats_connected.set(0)
            if self.nc and self.nc.is_connected:
                await self.nc.drain()
                await self.nc.close()
        finally:
            self.nc = None
            self.js = None
            self.kv = None

    async def publish_json(self, subject_suffix: str, payload: bytes) -> None:
        assert self.js is not None
        subject = f"{self.subject}.{subject_suffix}" if subject_suffix else self.subject
        attempt = 0
        while True:
            try:
                ack = await self.js.publish(subject, payload, timeout=2.0)
                if ack and ack.stream == self.stream:
                    posts_published_total.inc()
                    return
            except TimeoutError:
                publish_errors_total.inc()
                attempt += 1
                if attempt > self.max_retries:
                    log.error("publish_timeout", subject=subject)
                    raise
                await asyncio.sleep(0.1 * attempt)

    async def get_cursor(self, service_name: str) -> Optional[int]:
        if not self.kv:
            return None
        try:
            entry = await self.kv.get(service_name)
            if entry and entry.value:
                return int(entry.value.decode("utf-8"))
        except Exception:
            return None
        return None

    async def save_cursor(self, service_name: str, cursor: int) -> None:
        if not self.kv:
            return
        try:
            await self.kv.put(service_name, str(cursor).encode("utf-8"))
        except Exception as e:
            log.warning("kv_put_failed", error=str(e))
