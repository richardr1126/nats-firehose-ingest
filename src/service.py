import asyncio
import json
import signal
from collections import defaultdict
from typing import Any, Dict
import time

import uvicorn

from .config import settings
from .filters import PostFilter
from .firehose import FirehoseRunner
from .logging_setup import get_logger
from .metrics import operations_total, messages_per_second, events_per_second, firehose_cursor, ops_queue_size
from .nats_client import NatsClient
from .health import create_health_api


log = get_logger(__name__)


class Service:
    """Encapsulates the ingest service lifecycle and processing loops.

    The runtime behavior and metrics are kept identical to the previous module-level
    implementation, but responsibilities are grouped into methods for clarity and
    easier testing.
    """

    def __init__(self) -> None:
        # Logging is configured by the application's entry point; avoid
        # reconfiguring it here to prevent duplicate or cached logger issues.
        log.info("service_start", service=settings.SERVICE_NAME)

        # Shared queue for ops from firehose
        self.ops_queue: asyncio.Queue[defaultdict] = asyncio.Queue(maxsize=10000)

        # NATS client
        self.nats = NatsClient(
            url=settings.NATS_URL,
            stream=settings.NATS_STREAM,
            subject=settings.NATS_SUBJECT,
            kv_bucket=settings.NATS_KV_BUCKET,
            max_retries=settings.MAX_RETRIES,
        )

        # Firehose runner will be created after loading cursor
        self.firehose: FirehoseRunner | None = None

        # Filtering
        self.post_filter = PostFilter(
            enable_text_filtering=settings.ENABLE_TEXT_FILTERING,
            min_len=settings.MIN_TEXT_LENGTH,
            max_len=settings.MAX_TEXT_LENGTH,
        )

        # Web server
        app = create_health_api()
        config = uvicorn.Config(app, host="0.0.0.0", port=settings.HEALTH_CHECK_PORT, log_level="info")
        self.server = uvicorn.Server(config)

        # Lifecycle primitives
        self.stop_event = asyncio.Event()
        self.loop = asyncio.get_running_loop()

        # Producer metrics
        self.last_matched_count = 0  # Matched posts that passed filters
        self.last_events_count = 0   # Total events processed
        # Track when we last published metrics so we compute rates only when logging
        self._last_stats_time = time.monotonic()

        # Bookkeeping for cursor saves
        self._last_save = 0

        # Background tasks
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Connect services and start firehose and background loops."""
        await self.nats.connect()
        cursor = await self.nats.get_cursor(settings.SERVICE_NAME)

        # Create firehose runner and start it
        def on_ops(operations: defaultdict) -> None:
            try:
                self.ops_queue.put_nowait(operations)
            except asyncio.QueueFull:
                # Backpressure: drop if overloaded (preserve original behavior)
                pass

        self.firehose = FirehoseRunner(settings.SERVICE_NAME, on_ops, initial_cursor=cursor)
        self.firehose.start()

        # Register signal handlers once the loop is running
        for sig in (signal.SIGINT, signal.SIGTERM):
            # add_signal_handler raises on Windows — we're on Unix/macOS here
            self.loop.add_signal_handler(sig, self._handle_signal)

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._process_ops()),
            asyncio.create_task(self._run_server()),
            asyncio.create_task(self._periodic_stats_logger()),
        ]

    def _handle_signal(self) -> None:
        self.stop_event.set()

    async def _run_server(self) -> None:
        # Keep the same uvicorn server behavior as before
        await self.server.serve()

    async def _process_ops(self) -> None:
        """Consume operation batches from the firehose and publish matching posts to NATS.

        This preserves the original processing, metrics updates and cursor-save
        behavior.
        """
        processed = 0
        while not self.stop_event.is_set():
            try:
                operations = await asyncio.wait_for(self.ops_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            # Extract total event count from firehose
            events_batch = operations.pop('_event_count', 0)
            self.last_events_count += events_batch

            # Increment operations counter for visibility
            count_batch = 0

            created_posts = operations.get("app.bsky.feed.post", {}).get("created", [])
            for post in created_posts:
                record = post.get("record")
                # The original code used getattr(record, "text", None). Use dict access to
                # retain behavior when record is a mapping while being clearer.
                text = None
                if isinstance(record, dict):
                    text = record.get("text")
                else:
                    # Fallback to attribute access to preserve behavior for non-dict records
                    text = getattr(record, "text", None)

                if not self.post_filter.allow(text):
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
                    await self.nats.publish_json("posts", data)
                except Exception as e:
                    log.error("publish_failed", error=str(e))
                count_batch += 1

            operations_total.inc(count_batch)
            processed += count_batch

            # Save cursor periodically via NATS KV using latest commit seq propagated by FirehoseRunner
            self._last_save += 1
            if self._last_save >= settings.CURSOR_SAVE_INTERVAL:
                self._last_save = 0
                try:
                    # firehose may be None only briefly during startup; guard just in case
                    if self.firehose is not None:
                        await self.nats.save_cursor(settings.SERVICE_NAME, self.firehose.cursor)
                except Exception:
                    # Preserve original behavior: swallow persistent save errors
                    pass

            # Update matched posts count
            self.last_matched_count += count_batch

    async def _periodic_stats_logger(self) -> None:
        # Log every 20 seconds
        while not self.stop_event.is_set():
            await asyncio.sleep(20)
            try:
                # Update gauges
                if self.firehose is not None:
                    firehose_cursor.set(float(self.firehose.cursor))
                ops_queue_size.set(self.ops_queue.qsize())

                # Compute elapsed time since last stats and update per-second rates here
                now = time.monotonic()
                elapsed = now - self._last_stats_time
                if elapsed <= 0:
                    elapsed = 1e-6

                mps_rate = round(self.last_matched_count / elapsed, 2)
                eps_rate = round(self.last_events_count / elapsed, 2)

                messages_per_second.set(mps_rate)
                events_per_second.set(eps_rate)

                # Reset counters after publishing rates
                self.last_matched_count = 0
                self.last_events_count = 0
                self._last_stats_time = now

                # Log stats
                log.info(
                    "ingest stats",
                    cursor=self.firehose.cursor if self.firehose is not None else None,
                    queue_size=self.ops_queue.qsize(),
                    matched_per_second=mps_rate,
                    events_per_second=eps_rate,
                )
            except Exception as e:
                log.warning("stats_log_failed", error=str(e))

    async def run(self) -> None:
        """Start the service and wait until stop signal; then perform a graceful shutdown."""
        await self.start()
        await self.stop_event.wait()

        # Stop firehose
        if self.firehose is not None:
            try:
                self.firehose.stop()
            except Exception:
                pass

        # Cancel background tasks
        for t in self._tasks:
            t.cancel()

        # Close NATS connection
        await self.nats.close()
        log.info("service_stop")
