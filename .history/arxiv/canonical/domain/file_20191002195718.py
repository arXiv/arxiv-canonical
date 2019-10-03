"""Provides file-related concepts and logic."""

import os
from datetime import datetime
from typing import Any, Dict, IO, Iterable, NamedTuple, Optional, Union
from urllib.parse import urlparse

from typing_extensions import Protocol

from .base import CanonicalBase, Callback, with_callbacks
from .content import ContentType


class URI(str):
    def __new__(cls, value: str) -> 'URI':
        print('__new__', value)
        if value.startswith('/'):
            value = f'file:///{value.lstrip("/")}'
        uri: URI = super(URI, cls).__new__(cls, value)  # type: ignore
        return uri

    def __init__(self, value: str) -> None:
        if value.startswith('/'):
            value = f'file:///{value.lstrip("/")}'
        o = urlparse(value)
        self.scheme = o.scheme
        self.netloc = o.netloc
        self.path = o.path
        self.params = o.params
        self.query = o.query
        self.fragment = o.fragment

    @property
    def is_canonical(self) -> bool:
        return bool(self.scheme == 'arxiv')

    @property
    def is_file(self) -> bool:
        return bool(self.scheme == 'file')

    @property
    def is_http_url(self) -> bool:
        return bool(self.scheme == 'http' or self.scheme == 'https')


class Key(URI):
    def __new__(cls, value: str) -> 'Key':
        if not value.startswith('arxiv:///'):
            value = f'arxiv:///{value.lstrip("/")}'
        key: Key = super(Key, cls).__new__(cls, value)  # type: ignore
        return key

    def __init__(self, value: str) -> None:
        super(Key, self).__init__(value)
        _, self.filename = os.path.split(self.path)


class CanonicalFile(CanonicalBase):
    """Represents a file in the canonical record, e.g. a source package."""

    created: datetime
    modified: datetime
    size_bytes: int
    content_type: ContentType
    filename: Optional[str]
    ref: URI
    # content: Optional[Union[IO[bytes], URI]]

    def __init__(self, created: datetime,
                 modified: datetime,
                 size_bytes: int,
                 content_type: ContentType,
                 ref: URI,
                 filename: Optional[str] = None) -> None:
        self.created = created
        self.modified = modified
        self.size_bytes = size_bytes
        self.content_type = content_type
        self.filename = filename
        self.ref = ref
        # self.content = content

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'CanonicalFile':
        return cls(
            created=datetime.fromisoformat(data['created']),  # type: ignore ; pylint: disable=no-member
            modified=datetime.fromisoformat(data['modified']),  # type: ignore ; pylint: disable=no-member
            size_bytes=data['size_bytes'],
            content_type=ContentType(data['content_type']),
            filename=data['filename'],
            ref=URI(data['ref'])
            # content=URI(data['content'])
        )

    @property
    def mime_type(self) -> str:
        return self.content_type.mime_type

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'created': self.created.isoformat(),
            'modified': self.modified.isoformat(),
            'size_bytes': self.size_bytes,
            'content_type': self.content_type.value,
            'filename': self.filename,
            'ref': self.ref,
            # 'content': self.content
        }



