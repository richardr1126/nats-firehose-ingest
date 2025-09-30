from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PostFilter:
    enable_text_filtering: bool
    min_len: int
    max_len: int

    def allow(self, text: str | None) -> bool:
        if text is None:
            return False
        length = len(text)
        if length < self.min_len or length > self.max_len:
            return False
        if not self.enable_text_filtering:
            return True

        # Placeholders for future logic (language detection, spam checks, etc.)
        # Currently permissive beyond length bounds.
        return True
