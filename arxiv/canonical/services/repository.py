from http import HTTPStatus as status
from typing import IO, Iterable, Tuple, Union
from urllib3.util.retry import Retry

import requests

from .. import domain as D
from .. import record as R
from ..register import ICanonicalSource


class RemoteRepository(ICanonicalSource):
    """Retrieves content from a remote arXiv repository."""

    def __init__(self, base_uri: str, retry: int = 3, backoff: int = 2,
                 retry_status: int = 5,
                 force_retry_on: Iterable[status] = (
                     status.INTERNAL_SERVER_ERROR,
                     status.BAD_GATEWAY,
                     status.SERVICE_UNAVAILABLE,
                     status.GATEWAY_TIMEOUT
                 )) -> None:
        self._base_uri = base_uri
        self._session = requests.Session()
        self._adapter = requests.adapters.HTTPAdapter(
            max_retries=Retry(
                total=retry * 3,  # This is just a fallback for odd cases.
                read=retry,
                connect=retry,
                backoff_factor=backoff,
                status_forcelist=[code.value for code in force_retry_on],
                status=retry_status
            )
        )
        self._session.mount('http://', self._adapter)
        self._session.mount('https://', self._adapter)

    def can_resolve(self, uri: D.URI) -> bool:
        return uri.is_canonical

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        raise NotImplementedError('Implement me!')

    def load_deferred(self, key: D.URI) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        raise NotImplementedError('Implement me!')
