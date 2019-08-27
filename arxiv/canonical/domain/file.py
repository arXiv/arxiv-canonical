"""Provides file-related concepts and logic."""

from typing import NamedTuple, Optional, IO
from datetime import datetime
from typing_extensions import Protocol

from .content import ContentType


class File(NamedTuple):
    """Represents a file in the canonical record, e.g. a source package."""

    filename: str
    mime_type: str
    created: datetime
    modified: datetime
    size_bytes: int
    content_type: ContentType
    content: Optional[IO[bytes]] = None

    def with_content(self, content: Optional[IO[bytes]]) -> 'File':
      """Generate a copy of this :class:`.File` with ``content``."""
      return self._replace(content=content)