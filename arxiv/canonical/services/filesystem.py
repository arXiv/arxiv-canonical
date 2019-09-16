from http import HTTPStatus as status
from typing import IO, Iterable, Tuple, Union
from urllib3.util.retry import Retry

import requests

from .. import domain as D
from .. import record as R
from ..register import ICanonicalSource


class Filesystem(ICanonicalSource):
    """Retrieves content from a filesystem (outside the canonical record)."""

    def __init__(self, base_path: str) -> None:
        self._base_path = base_path

    def can_resolve(self, uri: D.URI) -> bool:
        return uri.is_file

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        raise NotImplementedError('Implement me!')

    def load_deferred(self, key: D.URI) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        raise NotImplementedError('Implement me!')
