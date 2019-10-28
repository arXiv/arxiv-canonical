"""Provides bitstream-related concepts and logic."""

import os
from datetime import datetime
from typing import Any, Dict, IO, Iterable, NamedTuple, Optional, Union
from urllib.parse import urlparse

from typing_extensions import Protocol

from .base import CanonicalBase
from .content import ContentType


class URI(str):
    """
    A unique identifier for bitstream content.

    Bitstream content may be located in a variety of places prior to
    canonicalization. For example, it may be located on a local filesystem,
    or at a remote location accessible via HTTP.
    """

    def __new__(cls, value: str) -> 'URI':
        """Make a new URI."""
        if value.startswith('/'):
            value = f'file:///{value.lstrip("/")}'
        uri: URI = super(URI, cls).__new__(cls, value)  # type: ignore
        return uri

    def __init__(self, value: str) -> None:
        """Initialize and parse an URI from a str value."""
        if value.startswith('/'):
            value = f'file:///{value.lstrip("/")}'

        o = urlparse(value)
        self.scheme = o.scheme
        if not self.scheme:
            raise ValueError(f'Not a valid URI: {value}')
        self.netloc = o.netloc
        self.path = o.path
        self.params = o.params
        self.query = o.query
        self.fragment = o.fragment

    @property
    def is_canonical(self) -> bool:
        """Indicate whether the URI is a key in the canonical record."""
        return bool(self.scheme == 'arxiv')

    @property
    def is_file(self) -> bool:
        """Indicate whether the URI is a path to a local file."""
        return bool(self.scheme == 'file')

    @property
    def is_http_url(self) -> bool:
        """Indicate whether the URI is an HTTP URL."""
        return bool(self.scheme == 'http' or self.scheme == 'https')


class Key(URI):
    """The unique identifier for a bitstream in the canonical record."""

    def __new__(cls, value: str) -> 'Key':
        """Make a new key."""
        if not value.startswith('arxiv:///'):
            value = f'arxiv:///{value.lstrip("/")}'
        key: Key = super(Key, cls).__new__(cls, value)  # type: ignore
        return key

    def __init__(self, value: str) -> None:
        """Initialize a key with a str value."""
        if not value.startswith('arxiv:///'):
            value = f'arxiv:///{value.lstrip("/")}'
        super(Key, self).__init__(value)
        _, self.filename = os.path.split(self.path)


class CanonicalFile(CanonicalBase):
    """Represents a file in the canonical record, e.g. a source package."""

    modified: datetime
    """Last time the file was modified."""

    size_bytes: int
    """Size of the file in bytes."""

    content_type: ContentType
    """The content type of the file."""

    filename: Optional[str]
    """Filename in the canonical record."""

    ref: URI
    """A reference to the location of the content of the file."""

    is_gzipped: bool
    """Whether or not the content at ``ref`` is served in gzipped form."""

    exclude_from_comparison = {'ref', 'is_gzipped'}

    def __init__(self, modified: datetime,
                 size_bytes: int,
                 content_type: ContentType,
                 ref: URI,
                 filename: Optional[str] = None,
                 is_gzipped: bool = False) -> None:
        self.modified = modified
        self.size_bytes = size_bytes
        self.content_type = content_type
        self.filename = filename
        self.ref = ref
        self.is_gzipped = is_gzipped

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CanonicalFile':
        """Reconstitute a :class:`.CanonicalFile` from a native dict."""
        return cls(
            modified=datetime.fromisoformat(data['modified']),  # type: ignore ; pylint: disable=no-member
            size_bytes=data['size_bytes'],
            content_type=ContentType(data['content_type']),
            filename=data['filename'],
            ref=URI(data['ref']),
            is_gzipped=data.get('is_gzipped', False)
        )

    @property
    def mime_type(self) -> str:
        """Convenience accessor for the MIME type of the file."""
        return self.content_type.mime_type

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict from this :class:`.CanonicalFile`."""
        return {
            'modified': self.modified.isoformat(),
            'size_bytes': self.size_bytes,
            'content_type': self.content_type.value,
            'filename': self.filename,
            'ref': self.ref,
            'is_gzipped': self.is_gzipped
        }
