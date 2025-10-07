"""Type definitions for the firehose ingest service."""

from typing import TypedDict, Optional


class RawPost(TypedDict):
    """Raw post data structure from Bluesky firehose."""
    uri: str
    cid: str
    author: str  # DID of the author
    text: str
    created_at: str  # ISO 8601 timestamp
