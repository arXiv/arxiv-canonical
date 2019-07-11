"""
Serialization and deserialization of the canonical record.

"""

import io
from json import dumps, loads
from typing import NamedTuple, IO, Iterable, Tuple, Optional
from hashlib import md5
from base64 import urlsafe_b64encode

from typing_extensions import Protocol

from ..encoder import CanonicalJSONEncoder


class ValidationError(Exception):
    """A data consistency problem was encountered."""


class ChecksumError(ValidationError):
    """An unexpected checksum value was encountered."""


class IEntry(Protocol):
    content_type: str

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

    key: str

    content: IO
    """Raw content of the entry."""

    @property
    def checksum(self) -> str:
        """URL-safe base64-encoded MD5 hash of the entry content."""
        return checksum(self.content)


def checksum(content: IO[bytes]) -> str:
    """Generate an URL-safe base64-encoded md5 hash of an IO."""
    content.seek(0)     # Make sure that we are at the start of the stream.
    hash_md5 = md5()
    for chunk in iter(lambda: content.read(4096), b""):
        hash_md5.update(chunk)
    content.seek(0)     # Be a good neighbor for subsequent users.
    return urlsafe_b64encode(hash_md5.digest()).decode('utf-8')

