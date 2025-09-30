"""Simplified synchronous service for firehose consumption and NATS publishing."""

import signal
import sys
import time
import threading
from typing import Dict, Any, List
from collections import defaultdict
import asyncio
import logging

import structlog
from atproto import models

from .config import settings
from .nats_client import NATSJetStreamClient, PostMessage
from .firehose_client import FirehoseClient
from .filters import filter_posts, get_filter_stats
from .health import HealthCheckServer

logger = structlog.get_logger(__name__)


class FirehoseIngestService:
    """Simplified main service for ingesting Bluesky firehose data into NATS JetStream."""
    
    def __init__(self):
        self.nats_client = NATSJetStreamClient()
        self.health_server = HealthCheckServer(self)
        self.firehose_client: FirehoseClient = None
        self.running = False
        self.shutdown_requested = False
        
        # Statistics
        self.total_operations_processed = 0
        self.posts_published = 0
        self.publish_errors = 0
        
        # Health check runs in separate thread with its own asyncio loop
        self.health_thread = None
        self.health_loop = None
        
        # Configure structured logging
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="ISO"),
                structlog.processors.add_log_level,
                structlog.processors.JSONRenderer() if settings.log_format == "json" 
                else structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(
                getattr(logging, settings.log_level.upper(), logging.INFO)
            ),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
    
    def start(self) -> None:
        """Start the firehose ingest service synchronously."""
        logger.info("Starting NATS Firehose Ingest Service", 
                   service=settings.service_name,
                   nats_url=settings.nats_url,
                   nats_stream=settings.nats_stream)
        
        try:
            # Connect to NATS (this will use asyncio.run internally)
            self._connect_nats()
            
            # Start health check server in separate thread
            self._start_health_server()
            
            # Set up signal handlers
            self._setup_signal_handlers()
            
            # Create firehose client with simple callback
            self.firehose_client = FirehoseClient(self._process_operations_sync)
            
            self.running = True
            logger.info("Service started successfully")
            
            # Start firehose client (this will block synchronously)
            self.firehose_client.start()
            
        except Exception as e:
            logger.error("Failed to start service", error=str(e))
            self.shutdown()
            raise
    
    def shutdown(self) -> None:
        """Shutdown the service gracefully."""
        if self.shutdown_requested:
            return
        
        self.shutdown_requested = True
        logger.info("Shutting down service")
        
        # Stop firehose client
        if self.firehose_client:
            self.firehose_client.stop()
        
        # Stop health server
        self._stop_health_server()
        
        # Disconnect from NATS
        self._disconnect_nats()
        
        self.running = False
        logger.info("Service shutdown complete")
    
    def _connect_nats(self) -> None:
        """Connect to NATS synchronously."""
        self.nats_client.connect_sync()
        logger.info("Connected to NATS")
    
    def _disconnect_nats(self) -> None:
        """Disconnect from NATS synchronously."""
        self.nats_client.disconnect_sync()
        logger.info("Disconnected from NATS")
    
    def _start_health_server(self) -> None:
        """Start health server in separate thread with its own asyncio loop."""
        def health_thread_target():
            # Create new event loop for this thread
            self.health_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.health_loop)
            
            try:
                # Start health server in this loop
                self.health_loop.run_until_complete(self.health_server.start())
                # Keep the loop running
                self.health_loop.run_forever()
            except Exception as e:
                logger.error("Health server thread failed", error=str(e))
            finally:
                self.health_loop.close()
        
        self.health_thread = threading.Thread(target=health_thread_target, daemon=True)
        self.health_thread.start()
        
        # Give it a moment to start
        time.sleep(0.5)
        logger.info("Health server started in separate thread")
    
    def _stop_health_server(self) -> None:
        """Stop health server."""
        if self.health_loop and self.health_loop.is_running():
            # Schedule stop in health loop
            asyncio.run_coroutine_threadsafe(
                self.health_server.stop(), 
                self.health_loop
            )
            
            # Stop the event loop
            self.health_loop.call_soon_threadsafe(self.health_loop.stop)
        
        if self.health_thread and self.health_thread.is_alive():
            self.health_thread.join(timeout=5.0)
        
        logger.info("Health server stopped")
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info("Received shutdown signal", signal=signum)
            self.shutdown()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _process_operations_sync(self, operations: Dict[str, Dict[str, List]]) -> None:
        """Handle operations from the firehose synchronously."""
        try:
            self.total_operations_processed += 1
            
            # Process posts for the specific collection (app.bsky.feed.post)
            for collection, ops in operations.items():
                created_posts = ops.get('created', [])
                
                if created_posts:
                    logger.debug("Processing posts", collection=collection, count=len(created_posts))
                    
                    posts_to_publish = []
                    for post_message in created_posts:
                        # Apply filters
                        if filter_posts([post_message]):
                            posts_to_publish.append(post_message)
                    
                    # Publish all filtered posts synchronously
                    if posts_to_publish:
                        for post in posts_to_publish:
                            self._publish_post_sync(post)
            
            # Log statistics periodically
            if self.total_operations_processed % 100 == 0:
                self._log_statistics()
                
        except Exception as e:
            logger.error("Failed to process operations", error=str(e), exc_info=True)
    
    def _publish_post_sync(self, post: PostMessage) -> None:
        """Publish a single post to NATS synchronously."""
        try:
            logger.debug("Publishing post to NATS", uri=post.uri, author=post.author_did)
            
            # Use synchronous publish method
            success = self.nats_client.publish_post_sync(post)
            
            if success:
                self.posts_published += 1
                logger.debug("Post published successfully", uri=post.uri)
            else:
                self.publish_errors += 1
                logger.warning("Post publish returned False", uri=post.uri)
                
        except Exception as e:
            logger.error("Failed to publish post", 
                        uri=post.uri, 
                        error=str(e),
                        exc_info=True)
            self.publish_errors += 1
    
    def _log_statistics(self) -> None:
        """Log service statistics."""
        nats_stats = self.nats_client.get_stats()
        firehose_stats = self.firehose_client.get_stats() if self.firehose_client else {}
        filter_stats = get_filter_stats()
        
        # Get cursor status synchronously
        cursor_status = {}
        if self.firehose_client and self.firehose_client.cursor_state:
            try:
                async def get_cursor_status():
                    return await self.firehose_client.cursor_state.get_cursor_status()
                
                cursor_status = asyncio.run(get_cursor_status())
            except Exception as e:
                cursor_status = {"error": str(e)}
        
        logger.info("Service statistics",
                   service_stats={
                       "operations_processed": self.total_operations_processed,
                       "posts_published": self.posts_published,
                       "publish_errors": self.publish_errors,
                       "running": self.running,
                   },
                   nats_stats=nats_stats,
                   firehose_stats=firehose_stats,
                   filter_stats=filter_stats,
                   cursor_status=cursor_status)
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get service health status."""
        nats_healthy = self.nats_client.connected
        firehose_healthy = (
            self.firehose_client.is_healthy() 
            if self.firehose_client else False
        )
        
        return {
            "service": "nats-firehose-ingest",
            "status": "healthy" if (nats_healthy and firehose_healthy and self.running) else "unhealthy",
            "timestamp": time.time(),
            "components": {
                "nats": {
                    "status": "healthy" if nats_healthy else "unhealthy",
                    "connected": nats_healthy,
                    "stats": self.nats_client.get_stats(),
                },
                "firehose": {
                    "status": "healthy" if firehose_healthy else "unhealthy",
                    "running": self.firehose_client.running if self.firehose_client else False,
                    "stats": self.firehose_client.get_stats() if self.firehose_client else {},
                },
                "filters": {
                    "status": "healthy",
                    "stats": get_filter_stats(),
                },
            },
            "service_stats": {
                "operations_processed": self.total_operations_processed,
                "posts_published": self.posts_published,
                "publish_errors": self.publish_errors,
                "uptime_seconds": (
                    self.firehose_client.get_stats().get("uptime_seconds", 0)
                    if self.firehose_client else 0
                ),
            },
        }
    
    def perform_health_check(self) -> bool:
        """Perform an active health check synchronously."""
        try:
            # Check NATS connection
            if not self.nats_client.health_check_sync():
                return False
            
            # Check firehose client
            if not self.firehose_client or not self.firehose_client.is_healthy():
                return False
            
            return True
            
        except Exception as e:
            logger.error("Health check failed", error=str(e))
            return False


def main():
    """Main entry point for the service (synchronous)."""
    service = FirehoseIngestService()
    
    try:
        service.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Service failed", error=str(e))
        sys.exit(1)
    finally:
        service.shutdown()


if __name__ == "__main__":
    main()