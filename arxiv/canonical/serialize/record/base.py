"""
Serialization and deserialization of the canonical record.

"""

import io
from json import dumps, loads
from typing import NamedTuple, IO, Iterable, Tuple
from hashlib import md5
from base64 import urlsafe_b64encode

from typing_extensions import Protocol

from ..encoder import CanonicalJSONEncoder


class IEntry(Protocol):
    @property
    def content(self) -> IO:
        """Raw content of the entry."""
        ...

    @property
    def checksum(self) -> str:
        """URL-safe base64-encoded MD5 hash of the entry content."""
        ...

    @property
    def key(self) -> str:
        ...


class BaseEntry(NamedTuple):
    """A single bitstream in the record."""

    year: int
    """The year in which the first version of the e-print was announced."""

    month: int
    """The month in which the first version of the e-print was announced."""

    content: IO
    """Raw content of the entry."""

    checksum: str
    """URL-safe base64-encoded MD5 hash of the entry content."""

    @property
    def key(self) -> str:
        """Key for this entry relative to the e-print base key."""
        raise NotImplementedError('Must be implemented by child class')


class BaseDailyEntry(NamedTuple):
    year: int

    month: int

    day: int

    content: IO
    """Raw content of the entry."""

    checksum: str
    """URL-safe base64-encoded MD5 hash of the entry content."""

    @property
    def key(self) -> str:
        raise NotImplementedError('Must be implemented in child class')


def checksum(content: IO[bytes]) -> str:
    """Generate an URL-safe base64-encoded md5 hash of an IO."""
    hash_md5 = md5()
    for chunk in iter(lambda: content.read(4096), b""):
        hash_md5.update(chunk)
    content.seek(0)
    return urlsafe_b64encode(hash_md5.digest()).decode('utf-8')

