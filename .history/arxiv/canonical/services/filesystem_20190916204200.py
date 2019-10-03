import io
import os
from http import HTTPStatus as status
from typing import IO, Iterable, Tuple, Union
from urllib3.util.retry import Retry

import requests

from .. import domain as D
from .. import record as R
from ..register import ICanonicalSource
from .readable import MemoizedReadable


class Filesystem(ICanonicalSource):
    """Retrieves content from a filesystem (outside the canonical record)."""

    def __init__(self, base_path: str) -> None:
        self._base_path = base_path

    def can_resolve(self, uri: D.URI) -> bool:
        return uri.is_file and self._base_path in os.path.normpath(uri.path)

    def load_entry(self, uri: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        raise NotImplementedError('Implement me!')

    def load_deferred(self, uri: D.URI) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        return open(os.path.normpath(uri.path), 'rb')
