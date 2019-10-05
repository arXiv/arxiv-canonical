from http import HTTPStatus as status
from typing import IO, Iterable, Tuple, Union
from urllib3.util.retry import Retry

import requests

from .. import domain as D
from .. import record as R
from ..register import ICanonicalSource


class RemoteSource(ICanonicalSource):
    """Retrieves content from remote URIs."""

    def __init__(self, trusted_domain: str,
                 retry: int = 3, backoff: int = 2, retry_status: int = 5,
                 force_retry_on: Iterable[status] = (
                     status.INTERNAL_SERVER_ERROR,
                     status.BAD_GATEWAY,
                     status.SERVICE_UNAVAILABLE,
                     status.GATEWAY_TIMEOUT
                 )) -> None:
        self._trusted_domain = trusted_domain
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
        print(uri, uri.netloc)
        return bool(uri.is_http_url and uri.netloc == self._trusted_domain)

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        raise NotImplementedError('Implement me!')

    def load_deferred(self, key: D.URI) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        raise NotImplementedError('Implement me!')