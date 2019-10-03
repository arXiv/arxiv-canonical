from http import HTTPStatus as status
from typing import IO, Iterable, Tuple, Union
from urllib.parse import urljoin
from urllib3.util.retry import Retry

import requests

from .. import domain as D
from .. import record as R
from ..register import ICanonicalSource
from ..remote import RemoteSource


class RemoteRepository(RemoteSource):
    """Retrieves content from a remote arXiv repository."""

    def can_resolve(self, uri: D.URI) -> bool:
        return uri.is_canonical

    def _to_http(self, uri: D.URI) -> D.URI:
        """Make an HTTP URI from an arXiv canonical URI."""
        return D.URI(urljoin(
            f'{self._trusted_scheme}://{self._trusted_domain}',
            uri.path
        ))

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        raise NotImplementedError('Implement me!')

    def load_deferred(self, key: D.URI) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        return super(RemoteRepository, self).load_deferred(self._to_http(key))
