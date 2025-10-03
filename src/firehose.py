import json
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from time import time
from typing import Callable, Optional
from atproto import (
    AtUri,
    CAR,
    firehose_models,
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)

from .logging_setup import get_logger

log = get_logger(__name__)

_INTERESTED_RECORDS = {
    models.AppBskyFeedPost: models.ids.AppBskyFeedPost,
}

def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> tuple[defaultdict, int]:
    """Extract operations and return (operations_dict, total_event_count)"""
    operations_by_type = defaultdict(lambda: {"created": [], "deleted": []})
    car = CAR.from_bytes(commit.blocks)
    
    total_events = 0
    for op in commit.ops:
        if op.action == "update":
            continue
        
        total_events += 1
        uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")
        
        if op.action == "delete":
            operations_by_type[uri.collection]["deleted"].append({"uri": str(uri)})
            continue
        if op.action == "create" and op.cid and uri.collection in _INTERESTED_RECORDS.values():
            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue
            try:
                record = models.get_or_create(record_raw_data, strict=False)
                create_info = {"uri": str(uri), "cid": str(op.cid), "author": commit.repo}
                operations_by_type[uri.collection]["created"].append({"record": record, **create_info})
            except Exception as e:
                log.error("parse_record_failed", error=str(e))
                continue
    del car
    return operations_by_type, total_events
class FirehoseRunner:
    def __init__(
            self,
            service_name: str,
            on_ops: Callable[[defaultdict], None],
            initial_cursor: Optional[int] = None
        ):
        self.service_name = service_name
        self.on_ops = on_ops
        self.cursor = initial_cursor or None
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=self.cursor)
        self.client = FirehoseSubscribeReposClient(params)
        self._thread: Optional[threading.Thread] = None

    def start(self):
        def on_message(message: firehose_models.MessageFrame) -> None:
            try:
                commit = parse_subscribe_repos_message(message)
            except Exception as e:
                log.error("parse_message_failed", error=str(e))
                return

            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                return
            if not commit.blocks:
                return

            # Periodic cursor update logging handled by orchestrator; we just forward ops
            # Guard against cursor regression due to out-of-order or replayed frames
            try:
                if self.cursor is None or (isinstance(commit.seq, int) and commit.seq > self.cursor):
                    self.cursor = commit.seq
            except Exception:
                # In case of unexpected types, fall back to direct assignment
                self.cursor = commit.seq
            ops, total_events = _get_ops_by_type(commit)
            # Add event count to operations dict
            ops['_event_count'] = total_events
            # Pass the cursor along with the batch to avoid cross-thread reads
            ops['_cursor'] = commit.seq
            self.on_ops(ops)

        self._thread = threading.Thread(target=lambda: self.client.start(on_message), daemon=True)
        self._thread.start()

    def stop(self):
        try:
            self.client.stop()
        except Exception:
            pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
