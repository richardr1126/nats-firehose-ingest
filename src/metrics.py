from __future__ import annotations

from prometheus_client import Counter, Gauge


operations_total = Counter(
    "nats_firehose_operations_total",
    "Total operations processed",
)

posts_published_total = Counter(
    "nats_firehose_posts_published_total",
    "Posts successfully published",
)

publish_errors_total = Counter(
    "nats_firehose_publish_errors_total",
    "Total publish errors",
)

messages_per_second = Gauge(
    "firehose_messages_per_second",
    "Current firehose matched posts rate (posts/sec)",
)

events_per_second = Gauge(
    "firehose_events_per_second",
    "Current firehose total events rate (events/sec)",
)

nats_connected = Gauge(
    "nats_connected",
    "NATS connection status (1=connected, 0=disconnected)",
)

# Additional gauges for observability
firehose_cursor = Gauge(
    "firehose_cursor",
    "Latest firehose cursor sequence",
)

ops_queue_size = Gauge(
    "firehose_ops_queue_size",
    "Current firehose operations queue size",
)
