"""Simplified synchronous ATProto firehose client for consuming Bluesky real-time data."""

import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Any
import asyncio

from atproto import (
    AtUri,
    CAR,
    firehose_models,
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)
from atproto.exceptions import FirehoseError
import structlog

from .config import settings
from .nats_client import PostMessage
from .cursor_store import PersistentCursorState, NATSCursorStore

logger = structlog.get_logger(__name__)


class FirehoseClient:
    """Simplified synchronous ATProto firehose client for real-time Bluesky data consumption."""
    
    def __init__(self, operations_callback: Callable[[Dict], None]):
        self.operations_callback = operations_callback
        
        # Initialize cursor store (will be set up during start)
        self.cursor_state: Optional[PersistentCursorState] = None
        self._cursor_nats_client = None
        
        self.client: Optional[FirehoseSubscribeReposClient] = None
        self.running = False
        self.stop_requested = False
        
        # Statistics
        self.messages_processed = 0
        self.commits_processed = 0
        self.posts_extracted = 0
        self.errors_count = 0
        self.start_time = time.time()
        
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        uptime = time.time() - self.start_time
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "messages_processed": self.messages_processed,
            "commits_processed": self.commits_processed,
            "posts_extracted": self.posts_extracted,
            "errors_count": self.errors_count,
            "current_cursor": self.cursor_state.get_cursor() if self.cursor_state else None,
            "messages_per_second": self.messages_processed / uptime if uptime > 0 else 0,
        }
    
    def _extract_operations(self, commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> Dict[str, List[Dict]]:
        """Extract and categorize operations from a commit."""
        operations = defaultdict(lambda: {"created": []})
        
        try:
            # Process CAR blocks
            car = CAR.from_bytes(commit.blocks)
            
            for op in commit.ops:
                # Skip updates and deletions - we only care about creates
                if op.action in ('update', 'delete'):
                    continue
                
                uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')
                
                # Handle creates - only process posts
                if (op.action == 'create' and 
                    op.cid and 
                    uri.collection == models.ids.AppBskyFeedPost):
                    
                    record_raw_data = car.blocks.get(op.cid)
                    if not record_raw_data:
                        continue
                    
                    try:
                        # Parse the record using the correct ATProto method
                        record = models.get_or_create(record_raw_data, strict=False)
                        
                        # Extract post data
                        post_data = {
                            "uri": str(uri),
                            "cid": str(op.cid),
                            "author": commit.repo,
                            "record": record,
                            "indexed_at": datetime.now(timezone.utc).isoformat(),
                        }
                        
                        operations[uri.collection]["created"].append(post_data)
                        
                    except Exception as e:
                        logger.warning("Failed to parse record", 
                                     uri=str(uri), 
                                     error=str(e))
                        continue
            
            return dict(operations)
            
        except Exception as e:
            logger.error("Failed to extract operations", 
                        repo=commit.repo, 
                        error=str(e))
            self.errors_count += 1
            return {}
    
    def _create_post_message(self, post_data: Dict) -> PostMessage:
        """Create a PostMessage from post data."""
        record = post_data["record"]
        
        # Extract reply information
        reply_parent = None
        reply_root = None
        if hasattr(record, 'reply') and record.reply:
            reply_parent = record.reply.parent.uri if record.reply.parent else None
            reply_root = record.reply.root.uri if record.reply.root else None
        
        # Extract media information
        has_media = (
            hasattr(record, 'embed') and 
            record.embed is not None
        )
        
        # Extract language
        language = None
        if hasattr(record, 'langs') and record.langs:
            language = record.langs[0] if len(record.langs) > 0 else None
        
        return PostMessage(
            uri=post_data["uri"],
            cid=post_data["cid"],
            author_did=post_data["author"],
            text=getattr(record, 'text', None),
            reply_parent=reply_parent,
            reply_root=reply_root,
            created_at=getattr(record, 'createdAt', None),
            indexed_at=post_data["indexed_at"],
            has_media=has_media,
            language=language,
        )
    
    def _on_message_handler(self, message: firehose_models.MessageFrame) -> None:
        """Handle incoming firehose messages synchronously."""
        if self.stop_requested:
            logger.info("Stop requested, stopping client")
            if self.client:
                self.client.stop()
            return
        
        self.messages_processed += 1
        
        try:
            # Parse the message
            commit = parse_subscribe_repos_message(message)
        except Exception as e:
            logger.error("Failed to parse message", error=str(e))
            self.errors_count += 1
            return
        
        # Only process commit messages
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        
        if not commit.blocks:
            return
        
        self.commits_processed += 1
        
        # Update cursor periodically
        if commit.seq % settings.cursor_save_interval == 0:
            current_time = time.time()
            last_interval_time = getattr(self, '_last_interval_time', self.start_time)
            elapsed = current_time - last_interval_time
            rate = settings.cursor_save_interval / elapsed if elapsed > 0 else 0
            
            logger.info("Processing firehose",
                       cursor=commit.seq,
                       rate_per_second=f"{rate:.2f}",
                       commits_processed=self.commits_processed,
                       posts_extracted=self.posts_extracted)
            
            self._last_interval_time = current_time
        
        # Update cursor synchronously
        if self.cursor_state:
            try:
                # Use asyncio.run to update cursor synchronously
                async def update_cursor():
                    await self.cursor_state.update_cursor(commit.seq)
                
                asyncio.run(update_cursor())
            except Exception as e:
                logger.warning("Failed to update cursor", error=str(e))
        
        # Extract operations
        operations = self._extract_operations(commit)
        
        if operations:
            # Convert to the format expected by the callback
            formatted_operations = defaultdict(lambda: {"created": []})
            
            for collection, ops in operations.items():
                # Process created posts only
                for post_data in ops["created"]:
                    post_message = self._create_post_message(post_data)
                    formatted_operations[collection]["created"].append(post_message)
                    self.posts_extracted += 1
            
            # Call the operations callback directly (synchronous)
            if formatted_operations:
                try:
                    self.operations_callback(dict(formatted_operations))
                except Exception as e:
                    logger.error("Operations callback failed", error=str(e))
                    self.errors_count += 1
    
    def start(self) -> None:
        """Start the firehose client synchronously."""
        logger.info("Starting firehose client", service=settings.service_name)
        
        # Initialize cursor store
        self._initialize_cursor_store()
        
        while not self.stop_requested:
            try:
                self._run_client()
            except FirehoseError as e:
                logger.error("Firehose error, retrying", error=str(e))
                self.errors_count += 1
                time.sleep(5)  # Wait before retry
            except Exception as e:
                logger.error("Unexpected error", error=str(e))
                self.errors_count += 1
                time.sleep(10)  # Longer wait for unexpected errors
        
        logger.info("Firehose client stopped")
    
    def _initialize_cursor_store(self) -> None:
        """Initialize cursor store synchronously."""
        async def init_store():
            # Create a dedicated NATS client for the cursor store
            import nats
            nats_client = await nats.connect(settings.nats_url)
            
            kv_store = NATSCursorStore(nats_client)
            self.cursor_state = PersistentCursorState(settings.service_name, kv_store)
            await self.cursor_state.initialize()
            
            # Keep this connection open for cursor operations
            return nats_client
        
        # Initialize cursor store and keep the NATS client for later use
        self._cursor_nats_client = asyncio.run(init_store())
        
        logger.info("Cursor store initialized")
    
    def _run_client(self) -> None:
        """Run the firehose client with cursor management synchronously."""
        # Set up client parameters
        params = None
        if self.cursor_state and self.cursor_state.get_cursor():
            params = models.ComAtprotoSyncSubscribeRepos.Params(
                cursor=self.cursor_state.get_cursor()
            )
        
        # Initialize client
        self.client = FirehoseSubscribeReposClient(params)
        self.running = True
        
        try:
            # Start the client synchronously
            logger.info("Connecting to firehose", 
                       cursor=self.cursor_state.get_cursor() if self.cursor_state else None)
            self.client.start(self._on_message_handler)
        finally:
            self.running = False
            self.client = None
    
    def stop(self) -> None:
        """Stop the firehose client."""
        logger.info("Stopping firehose client")
        self.stop_requested = True
        
        # Force save cursor before stopping
        if self.cursor_state:
            try:
                async def force_save():
                    await self.cursor_state.force_save()
                
                asyncio.run(force_save())
            except Exception as e:
                logger.warning("Failed to save cursor during stop", error=str(e))
        
        # Close cursor NATS client
        if self._cursor_nats_client:
            try:
                async def close_cursor_nats():
                    await self._cursor_nats_client.close()
                
                asyncio.run(close_cursor_nats())
            except Exception as e:
                logger.warning("Failed to close cursor NATS client", error=str(e))
        
        if self.client:
            self.client.stop()
    
    def is_healthy(self) -> bool:
        """Check if the client is healthy."""
        if not self.running:
            return False
        
        # Check if we've processed messages recently
        if self.cursor_state:
            time_since_last_cursor_update = time.time() - self.cursor_state.last_updated
            return time_since_last_cursor_update < 300  # 5 minutes
        
        return True