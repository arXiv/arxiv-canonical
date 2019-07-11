"""Provides file-related concepts and logic."""

from typing import NamedTuple, Optional, IO
from datetime import datetime
from typing_extensions import Protocol


class File(NamedTuple):
    """Represents a file in the canonical record, e.g. a source package."""

    filename: str
    mime_type: str
    checksum: str
    created: datetime
    modified: datetime
    content: Optional[IO[bytes]] = None

    def with_content(self, content: Optional[IO[bytes]]) -> 'File':
      """Generate a copy of this :class:`.File` with ``content``."""
      return File(filename=self.filename,
                  mime_type=self.mime_type,
                  checksum=self.checksum,
                  created=self.created,
                  modified=self.modified,
                  content=content)