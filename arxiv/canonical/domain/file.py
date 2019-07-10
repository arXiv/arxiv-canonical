"""Provides file-related concepts and logic."""

from typing import NamedTuple, Optional
from datetime import datetime
from typing_extensions import Protocol


class Readable(Protocol):
    def read(self, size: int = -1) -> bytes:
        """
        Read raw bytes content from the resource.

        This should behave more or less like :func:`io.BufferedIOBase.read`.

        Examples might include:

        - A native Python ``file`` object;
        - A closure that, when called, creates a new ``file`` pointer and reads
          it;
        - A closure that, when called, makes an HTTP request and reads the
          resource.

        """
        ...


class File(NamedTuple):
    """Represents a file in the canonical record, e.g. a source package."""

    filename: str
    mime_type: str
    checksum: str
    created: datetime
    modified: datetime
    content: Optional[Readable] = None