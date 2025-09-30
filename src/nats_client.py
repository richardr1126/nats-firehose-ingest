"""NATS JetStream client for publishing Bluesky posts (sync/async compatible)."""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timezone
import threading

import nats
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig, ConsumerConfig, RetentionPolicy, StorageType
import structlog

from .config import settings

logger = structlog.get_logger(__name__)


@dataclass
class PostMessage:
    """Structured message for a Bluesky post."""
    uri: str
    cid: str
    author_did: str
    text: Optional[str]
    reply_parent: Optional[str] = None
    reply_root: Optional[str] = None
    created_at: Optional[str] = None
    indexed_at: Optional[str] = None
    labels: Optional[List[str]] = None
    has_media: bool = False
    language: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "uri": self.uri,
            "cid": self.cid,
            "author_did": self.author_did,
            "text": self.text,
            "reply_parent": self.reply_parent,
            "reply_root": self.reply_root,
            "created_at": self.created_at,
            "indexed_at": self.indexed_at or datetime.now(timezone.utc).isoformat(),
            "labels": self.labels or [],
            "has_media": self.has_media,
            "language": self.language,
        }
    
    def to_json(self) -> bytes:
        """Convert to JSON bytes for NATS publishing."""
        return json.dumps(self.to_dict()).encode('utf-8')


class NATSJetStreamClient:
    """NATS JetStream client for publishing Bluesky posts with sync/async support."""
    
    def __init__(self):
        self.nc: Optional[NATS] = None
        self.js = None
        self.connected = False
        self.publish_count = 0
        self.error_count = 0
        self.last_publish_time = 0
        
        # Connection lock for thread safety
        self._connection_lock = threading.Lock()
        
    async def connect(self) -> None:
        """Connect to NATS and set up JetStream."""
        with self._connection_lock:
            if self.connected:
                return
                
            try:
                logger.info("Connecting to NATS", nats_url=settings.nats_url)
                
                # Connect to NATS
                self.nc = await nats.connect(settings.nats_url)
                self.js = self.nc.jetstream()
                
                # Ensure stream exists
                await self._ensure_stream_exists()
                
                self.connected = True
                logger.info("Connected to NATS JetStream successfully", 
                           stream=settings.nats_stream, 
                           subject=settings.nats_subject)
                
            except Exception as e:
                logger.error("Failed to connect to NATS", error=str(e))
                self.connected = False
                raise
    
    async def disconnect(self) -> None:
        """Disconnect from NATS."""
        with self._connection_lock:
            if self.nc and not self.nc.is_closed:
                await self.nc.close()
                self.connected = False
                logger.info("Disconnected from NATS")
    
    def connect_sync(self) -> None:
        """Connect to NATS synchronously."""
        asyncio.run(self.connect())
    
    def disconnect_sync(self) -> None:
        """Disconnect from NATS synchronously."""
        try:
            asyncio.run(self.disconnect())
        except Exception as e:
            logger.warning("Error during synchronous disconnect", error=str(e))
    
    async def _ensure_stream_exists(self) -> None:
        """Ensure the JetStream stream exists."""
        try:
            # Try to get stream info
            await self.js.stream_info(settings.nats_stream)
            logger.info("Stream already exists", stream=settings.nats_stream)
        except Exception:
            # Stream doesn't exist, create it
            logger.info("Creating stream", stream=settings.nats_stream)
            
            stream_config = StreamConfig(
                name=settings.nats_stream,
                subjects=[f"{settings.nats_subject}.>"],
                retention=RetentionPolicy.LIMITS,
                storage=StorageType.FILE,
                max_age=24 * 60 * 60,  # 24 hours in seconds
                max_msgs=1000000,  # 1M messages
            )
            
            await self.js.add_stream(stream_config)
            logger.info("Stream created successfully", stream=settings.nats_stream)
    
    async def publish_post(self, post: PostMessage, timeout: float = 5.0) -> bool:
        """Publish a single post to NATS JetStream."""
        if not self.connected or not self.js:
            logger.error("Not connected to NATS", connected=self.connected, js_exists=self.js is not None)
            return False
        
        try:
            subject = f"{settings.nats_subject}.post"
            message_data = post.to_json()
            
            logger.debug("Attempting to publish to NATS", 
                        subject=subject, 
                        uri=post.uri,
                        message_size=len(message_data))
            
            # Publish with acknowledgment
            ack = await self.js.publish(
                subject=subject,
                payload=message_data,
                timeout=timeout
            )
            
            self.publish_count += 1
            self.last_publish_time = time.time()

            logger.debug("Published post", 
                        uri=post.uri, 
                        subject=subject,
                        ack_seq=ack.seq,
                        total_published=self.publish_count)
            
            return True
            
        except Exception as e:
            self.error_count += 1
            logger.error("Failed to publish post", 
                        uri=post.uri, 
                        error=str(e),
                        error_type=type(e).__name__,
                        exc_info=True)
            return False
    
    def publish_post_sync(self, post: PostMessage, timeout: float = 5.0) -> bool:
        """Publish a single post to NATS JetStream synchronously."""
        try:
            return asyncio.run(self.publish_post(post, timeout))
        except Exception as e:
            logger.error("Synchronous publish failed", uri=post.uri, error=str(e))
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            "connected": self.connected,
            "publish_count": self.publish_count,
            "error_count": self.error_count,
            "last_publish_time": self.last_publish_time,
            "success_rate": (
                self.publish_count / (self.publish_count + self.error_count)
                if (self.publish_count + self.error_count) > 0 else 0
            )
        }
    
    async def health_check(self) -> bool:
        """Perform a health check by publishing a test message."""
        if not self.connected:
            return False
        
        try:
            test_subject = f"{settings.nats_subject}.health"
            test_message = json.dumps({
                "type": "health_check",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": settings.service_name
            }).encode('utf-8')
            
            await self.js.publish(
                subject=test_subject,
                payload=test_message,
                timeout=2.0
            )
            
            return True
            
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return False
    
    def health_check_sync(self) -> bool:
        """Perform a health check synchronously."""
        try:
            return asyncio.run(self.health_check())
        except Exception as e:
            logger.error("Synchronous health check failed", error=str(e))
            return False
    
    async def ensure_connection(self) -> bool:
        """Ensure NATS connection is active, reconnect if needed."""
        if not self.connected or not self.nc or self.nc.is_closed:
            try:
                await self.connect()
                return True
            except Exception as e:
                logger.error("Failed to ensure connection", error=str(e))
                return False
        return True
    
    def ensure_connection_sync(self) -> bool:
        """Ensure NATS connection is active synchronously."""
        try:
            return asyncio.run(self.ensure_connection())
        except Exception as e:
            logger.error("Failed to ensure connection synchronously", error=str(e))
            return False