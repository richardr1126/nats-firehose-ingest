"""Data filtering logic for Bluesky posts."""

import re
from typing import List, Optional, Set
from datetime import datetime, timezone

import structlog

from .config import settings
from .nats_client import PostMessage

logger = structlog.get_logger(__name__)


class PostFilter:
    """Filter for Bluesky posts with configurable rules."""
    
    def __init__(self):
        # Basic content filters
        self.min_text_length = settings.min_text_length
        self.max_text_length = settings.max_text_length
        self.enable_text_filtering = settings.enable_text_filtering
        
        # Language filters (optional)
        self.allowed_languages: Optional[Set[str]] = None
        
        # Content patterns to exclude (spam, etc.)
        self.spam_patterns = [
            r'https?://bit\.ly/',  # Shortened URLs
            r'https?://tinyurl\.com/',
            r'🚀.*moon',  # Crypto spam patterns
            r'💎.*hands',
            r'buy.*now.*limited.*time',
        ]
        
        self.spam_regex = re.compile('|'.join(self.spam_patterns), re.IGNORECASE)
        
        # Statistics
        self.total_posts_processed = 0
        self.posts_accepted = 0
        self.posts_rejected = 0
        self.rejection_reasons = {
            'too_short': 0,
            'too_long': 0,
            'no_text': 0,
            'spam_detected': 0,
            'language_filtered': 0,
            'content_filtered': 0,
        }
    
    def set_language_filter(self, languages: List[str]) -> None:
        """Set allowed languages."""
        self.allowed_languages = set(languages) if languages else None
        logger.info("Language filter set", languages=languages)
    
    def should_include_post(self, post: PostMessage) -> bool:
        """Determine if a post should be included based on filtering rules."""
        self.total_posts_processed += 1
        
        # Skip if text filtering is disabled
        if not self.enable_text_filtering:
            self.posts_accepted += 1
            return True
        
        # Check if post has text
        if not post.text or not post.text.strip():
            self._reject_post('no_text')
            return False
        
        text = post.text.strip()
        
        # Check text length
        if len(text) < self.min_text_length:
            self._reject_post('too_short')
            return False
        
        if len(text) > self.max_text_length:
            self._reject_post('too_long')
            return False
        
        # Check for spam patterns
        if self.spam_regex.search(text):
            self._reject_post('spam_detected')
            return False
        
        # Check language filter
        if self.allowed_languages and post.language:
            if post.language not in self.allowed_languages:
                self._reject_post('language_filtered')
                return False
        
        # Additional content filters can be added here
        if self._is_content_filtered(text):
            self._reject_post('content_filtered')
            return False
        
        # Post passed all filters
        self.posts_accepted += 1
        return True
    
    def _reject_post(self, reason: str) -> None:
        """Record a post rejection."""
        self.posts_rejected += 1
        self.rejection_reasons[reason] += 1
    
    def _is_content_filtered(self, text: str) -> bool:
        """Additional content filtering logic."""
        # Check for excessive capitalization (possible spam)
        if len(text) > 20 and sum(1 for c in text if c.isupper()) / len(text) > 0.7:
            return True
        
        # Check for excessive emojis
        emoji_count = sum(1 for c in text if ord(c) > 0x1F600)
        if len(text) > 10 and emoji_count / len(text) > 0.3:
            return True
        
        # Check for repetitive characters
        if re.search(r'(.)\1{10,}', text):  # Same character repeated 10+ times
            return True
        
        return False
    
    def get_stats(self) -> dict:
        """Get filtering statistics."""
        acceptance_rate = (
            self.posts_accepted / self.total_posts_processed 
            if self.total_posts_processed > 0 else 0
        )
        
        return {
            'total_processed': self.total_posts_processed,
            'accepted': self.posts_accepted,
            'rejected': self.posts_rejected,
            'acceptance_rate': acceptance_rate,
            'rejection_reasons': self.rejection_reasons.copy(),
            'filters_enabled': {
                'text_filtering': self.enable_text_filtering,
                'language_filtering': self.allowed_languages is not None,
                'min_text_length': self.min_text_length,
                'max_text_length': self.max_text_length,
            }
        }
    
    def reset_stats(self) -> None:
        """Reset filtering statistics."""
        self.total_posts_processed = 0
        self.posts_accepted = 0
        self.posts_rejected = 0
        self.rejection_reasons = {key: 0 for key in self.rejection_reasons}


# Global filter instance
post_filter = PostFilter()


def configure_filters(
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    languages: Optional[List[str]] = None,
    enable_filtering: Optional[bool] = None
) -> None:
    """Configure global filters."""
    
    if min_length is not None:
        post_filter.min_text_length = min_length
    
    if max_length is not None:
        post_filter.max_text_length = max_length
    
    if languages is not None:
        post_filter.set_language_filter(languages)
    
    if enable_filtering is not None:
        post_filter.enable_text_filtering = enable_filtering
    
    logger.info("Filters configured",
               min_length=post_filter.min_text_length,
               max_length=post_filter.max_text_length,
               languages=list(post_filter.allowed_languages) if post_filter.allowed_languages else None,
               filtering_enabled=post_filter.enable_text_filtering)


def filter_post(post: PostMessage) -> bool:
    """Filter a single post using global filter."""
    return post_filter.should_include_post(post)


def filter_posts(posts: List[PostMessage]) -> List[PostMessage]:
    """Filter a list of posts using global filter."""
    return [post for post in posts if post_filter.should_include_post(post)]


def get_filter_stats() -> dict:
    """Get global filter statistics."""
    return {
        'post_filter': post_filter.get_stats()
    }


def reset_filter_stats() -> None:
    """Reset global filter statistics."""
    post_filter.reset_stats()