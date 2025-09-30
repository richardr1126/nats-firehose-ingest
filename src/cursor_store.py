"""NATS Key-Value store client for cursor persistence."""

import asyncio
from typing import Optional
import json
from datetime import datetime, timezone

import nats
from nats.aio.client import Client as NATS
from nats.js.api import KeyValueConfig, StorageType
import structlog

from .config import settings

logger = structlog.get_logger(__name__)


class NATSCursorStore:
    """NATS Key-Value store for persisting firehose cursor state."""
    
    def __init__(self, nats_client: NATS):
        self.nc = nats_client
        self.js = None
        self.kv = None
        self.bucket_name = settings.nats_kv_bucket
        self.service_key = f"{settings.service_name}.cursor"
        
    async def initialize(self) -> None:
        """Initialize the Key-Value store."""
        try:
            self.js = self.nc.jetstream()
            
            # Try to get existing bucket
            try:
                self.kv = await self.js.key_value(self.bucket_name)
                logger.info("Connected to existing KV bucket", bucket=self.bucket_name)
            except Exception:
                # Create bucket if it doesn't exist
                logger.info("Creating KV bucket", bucket=self.bucket_name)
                
                kv_config = KeyValueConfig(
                    bucket=self.bucket_name,
                    description="Firehose cursor state storage",
                    storage=StorageType.FILE,
                    ttl=7 * 24 * 60 * 60,  # 7 days TTL
                    max_bytes=1024 * 1024,  # 1MB max
                )
                
                self.kv = await self.js.create_key_value(config=kv_config)
                logger.info("Created KV bucket", bucket=self.bucket_name)
                
        except Exception as e:
            logger.error("Failed to initialize KV store", error=str(e))
            raise
    
    async def save_cursor(self, cursor: int) -> bool:
        """Save cursor position to NATS KV store."""
        if not self.kv:
            logger.warning("KV store not initialized")
            return False
        
        try:
            cursor_data = {
                "cursor": cursor,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": settings.service_name,
                "version": "1.0"
            }
            
            await self.kv.put(
                self.service_key, 
                json.dumps(cursor_data).encode('utf-8')
            )
            
            logger.debug("Cursor saved to KV store", 
                        cursor=cursor, 
                        key=self.service_key)
            return True
            
        except Exception as e:
            logger.error("Failed to save cursor", 
                        cursor=cursor, 
                        error=str(e))
            return False
    
    async def load_cursor(self) -> Optional[int]:
        """Load cursor position from NATS KV store."""
        if not self.kv:
            logger.warning("KV store not initialized")
            return None
        
        try:
            entry = await self.kv.get(self.service_key)
            
            if not entry or not entry.value:
                logger.info("No saved cursor found", key=self.service_key)
                return None
            
            cursor_data = json.loads(entry.value.decode('utf-8'))
            cursor = cursor_data.get("cursor")
            timestamp = cursor_data.get("timestamp")
            
            if cursor is not None:
                logger.info("Loaded cursor from KV store", 
                           cursor=cursor, 
                           saved_at=timestamp,
                           key=self.service_key)
                return int(cursor)
            
            return None
            
        except Exception as e:
            logger.error("Failed to load cursor", error=str(e))
            return None
    
    async def delete_cursor(self) -> bool:
        """Delete cursor from KV store (useful for reset)."""
        if not self.kv:
            logger.warning("KV store not initialized")
            return False
        
        try:
            await self.kv.delete(self.service_key)
            logger.info("Cursor deleted from KV store", key=self.service_key)
            return True
            
        except Exception as e:
            logger.error("Failed to delete cursor", error=str(e))
            return False
    
    async def get_cursor_info(self) -> Optional[dict]:
        """Get detailed cursor information."""
        if not self.kv:
            return None
        
        try:
            entry = await self.kv.get(self.service_key)
            
            if not entry or not entry.value:
                return None
            
            cursor_data = json.loads(entry.value.decode('utf-8'))
            
            # Add KV metadata
            cursor_data.update({
                "kv_sequence": entry.sequence,
                "kv_created": entry.created.isoformat() if entry.created else None,
                "kv_bucket": self.bucket_name,
                "kv_key": self.service_key,
            })
            
            return cursor_data
            
        except Exception as e:
            logger.error("Failed to get cursor info", error=str(e))
            return None


class PersistentCursorState:
    """Cursor state manager with NATS KV persistence."""
    
    def __init__(self, service_name: str, kv_store: NATSCursorStore):
        self.service_name = service_name
        self.kv_store = kv_store
        self.cursor: Optional[int] = None
        self.last_updated = 0
        self.last_saved = 0
        self.save_pending = False
        
    async def initialize(self) -> None:
        """Initialize and load cursor from KV store."""
        await self.kv_store.initialize()
        
        # Load saved cursor
        saved_cursor = await self.kv_store.load_cursor()
        if saved_cursor is not None:
            self.cursor = saved_cursor
            logger.info("Restored cursor from KV store", cursor=self.cursor)
        else:
            logger.info("No saved cursor found, starting fresh")
    
    async def update_cursor(self, new_cursor: int) -> None:
        """Update cursor position and save to KV store."""
        self.cursor = new_cursor
        self.last_updated = asyncio.get_event_loop().time()
        
        # Save every N messages or if enough time has passed
        should_save = (
            new_cursor % settings.cursor_save_interval == 0 or
            (self.last_updated - self.last_saved) > 30.0  # Save every 30 seconds
        )
        
        if should_save and not self.save_pending:
            self.save_pending = True
            # Save asynchronously to avoid blocking
            asyncio.create_task(self._save_cursor_async())
        
        # Log progress
        if new_cursor % settings.cursor_save_interval == 0:
            logger.info("Cursor updated", 
                       cursor=new_cursor, 
                       service=self.service_name)
    
    async def _save_cursor_async(self) -> None:
        """Save cursor asynchronously."""
        try:
            if self.cursor is not None:
                success = await self.kv_store.save_cursor(self.cursor)
                if success:
                    self.last_saved = asyncio.get_event_loop().time()
                    logger.debug("Cursor persisted", cursor=self.cursor)
        except Exception as e:
            logger.error("Failed to save cursor asynchronously", error=str(e))
        finally:
            self.save_pending = False
    
    async def force_save(self) -> bool:
        """Force save current cursor."""
        if self.cursor is not None:
            return await self.kv_store.save_cursor(self.cursor)
        return False
    
    def get_cursor(self) -> Optional[int]:
        """Get current cursor position."""
        return self.cursor
    
    async def reset_cursor(self) -> bool:
        """Reset cursor (delete from KV store)."""
        success = await self.kv_store.delete_cursor()
        if success:
            self.cursor = None
            self.last_updated = 0
            self.last_saved = 0
        return success
    
    async def get_cursor_status(self) -> dict:
        """Get detailed cursor status."""
        kv_info = await self.kv_store.get_cursor_info()
        
        return {
            "current_cursor": self.cursor,
            "last_updated": self.last_updated,
            "last_saved": self.last_saved,
            "save_pending": self.save_pending,
            "kv_info": kv_info,
        }