from __future__ import annotations

import asyncio
import json
import signal
from collections import defaultdict
from typing import Any, Dict

import uvicorn

from .config import settings
from .filters import PostFilter
from .firehose import FirehoseRunner
from .logging_setup import configure_logging, get_logger
from .metrics import operations_total, messages_per_second, events_per_second, firehose_cursor, ops_queue_size
from .nats_client import NatsService
from .health import create_health_api


log = get_logger(__name__)


async def service_main():
    configure_logging(settings.LOG_LEVEL, settings.LOG_FORMAT)
    log.info("service_start", service=settings.SERVICE_NAME)

    # Shared queue for ops from firehose
    ops_queue: asyncio.Queue[defaultdict] = asyncio.Queue(maxsize=10000)

    # NATS service
    nats = NatsService(
        url=settings.NATS_URL,
        stream=settings.NATS_STREAM,
        subject=settings.NATS_SUBJECT,
        kv_bucket=settings.NATS_KV_BUCKET,
        max_retries=settings.MAX_RETRIES,
    )
    await nats.connect()

    # Load cursor
    cursor = await nats.get_cursor(settings.SERVICE_NAME)

    # Firehose runner
    def on_ops(operations: defaultdict) -> None:
        try:
            ops_queue.put_nowait(operations)
        except asyncio.QueueFull:
            # Backpressure: drop if overloaded
            pass

    firehose = FirehoseRunner(settings.SERVICE_NAME, on_ops, initial_cursor=cursor)
    firehose.start()

    # Web server
    app = create_health_api()
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.HEALTH_CHECK_PORT, log_level="info")
    server = uvicorn.Server(config)

    # Filtering
    post_filter = PostFilter(
        enable_text_filtering=settings.ENABLE_TEXT_FILTERING,
        min_len=settings.MIN_TEXT_LENGTH,
        max_len=settings.MAX_TEXT_LENGTH,
    )

    stop_event = asyncio.Event()

    def _handle_signal():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Producer metrics
    last_matched_count = 0  # Matched posts that passed filters
    last_events_count = 0   # Total events processed
    window = 2.0

    async def process_ops():
        nonlocal last_matched_count, last_events_count
        processed = 0
        last_save = 0
        while not stop_event.is_set():
            try:
                operations = await asyncio.wait_for(ops_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            # Extract total event count from firehose
            events_batch = operations.pop('_event_count', 0)
            last_events_count += events_batch

            # Increment operations counter for visibility
            count_batch = 0

            created_posts = operations.get("app.bsky.feed.post", {}).get("created", [])
            for post in created_posts:
                record = post.get("record")
                text = getattr(record, "text", None)
                if not post_filter.allow(text):
                    continue

                # Build payload
                payload: Dict[str, Any] = {
                    "uri": post.get("uri"),
                    "cid": post.get("cid"),
                    "author": post.get("author"),
                    "text": text,
                }
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

                try:
                    await nats.publish_json("posts", data)
                except Exception as e:
                    log.error("publish_failed", error=str(e))
                count_batch += 1

            operations_total.inc(count_batch)
            processed += count_batch

            # Save cursor periodically via NATS KV using latest commit seq propagated by FirehoseRunner
            last_save += 1
            if last_save >= settings.CURSOR_SAVE_INTERVAL:
                last_save = 0
                try:
                    await nats.save_cursor(settings.SERVICE_NAME, firehose.cursor)
                except Exception:
                    pass

            # Update matched posts count
            last_matched_count += count_batch

    async def update_rate():
        nonlocal last_matched_count, last_events_count
        while not stop_event.is_set():
            await asyncio.sleep(window)
            messages_per_second.set(last_matched_count / window)
            events_per_second.set(last_events_count / window)
            last_matched_count = 0
            last_events_count = 0

    async def run_server():
        await server.serve()

    async def periodic_stats_logger():
        # Log every 20 seconds
        while not stop_event.is_set():
            await asyncio.sleep(20)
            try:
                # Update gauges
                firehose_cursor.set(float(firehose.cursor))
                ops_queue_size.set(ops_queue.qsize())
                mps = messages_per_second._value.get()
                eps = events_per_second._value.get()
                # Log stats
                log.info(
                    "stats",
                    cursor=firehose.cursor,
                    queue_size=ops_queue.qsize(),
                    matched_per_second=mps,
                    events_per_second=eps,
                )
            except Exception as e:
                log.warning("stats_log_failed", error=str(e))

    # Run tasks concurrently
    tasks = [
        asyncio.create_task(process_ops()),
        asyncio.create_task(update_rate()),
        asyncio.create_task(run_server()),
        asyncio.create_task(periodic_stats_logger()),
    ]

    await stop_event.wait()
    firehose.stop()
    for t in tasks:
        t.cancel()
    await nats.close()
    log.info("service_stop")
