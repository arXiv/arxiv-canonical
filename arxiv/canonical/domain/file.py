"""Provides file-related concepts and logic."""

from typing import NamedTuple, Optional, IO
from datetime import datetime
from typing_extensions import Protocol

from .content import ContentType


class CanonicalFile(NamedTuple):
    """Represents a file in the canonical record, e.g. a source package."""

    created: datetime
    modified: datetime
    size_bytes: int
    content_type: ContentType
    filename: Optional[str] = None
    content: Optional[IO[bytes]] = None

    @property
    def mime_type(self) -> str:
        return self.content_type.mime_type

    def with_content(self, content: Optional[IO[bytes]]) -> 'File':
        """Generate a copy of this :class:`.File` with ``content``."""
        return self._replace(content=content)