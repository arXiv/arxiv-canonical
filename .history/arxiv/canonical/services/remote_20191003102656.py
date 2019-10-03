import io

from http import HTTPStatus as status
from typing import Any, Callable, Iterable, IO, Optional, Tuple, Union
from urllib3.util.retry import Retry

import requests

from .. import domain as D
from .. import record as R
from ..register import ICanonicalSource
from .readable import ReadWrapper, MemoizedReadable


class RemoteSource(ICanonicalSource):
    """Retrieves content from remote URIs."""

    def __init__(self, trusted_domain: str, trusted_scheme: str = 'https',
                 retry: int = 3, backoff: int = 2, retry_status: int = 5,
                 force_retry_on: Iterable[status] = (
                     status.INTERNAL_SERVER_ERROR,
                     status.BAD_GATEWAY,
                     status.SERVICE_UNAVAILABLE,
                     status.GATEWAY_TIMEOUT
                 )) -> None:

        self._trusted_scheme = trusted_scheme
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
        return bool(uri.is_http_url
                    and uri.netloc == self._trusted_domain
                    and uri.scheme == self._trusted_scheme)

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        raise NotImplementedError('Implement me!')

    def load_deferred(self, key: D.URI, stream: bool = True) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        if not self.can_resolve(key):
            raise RuntimeError(f'Cannot resolve URI: {key}')
        return DeferredRequestReader(self._session.get, key, stream=stream)


class DeferredRequestReader(io.BytesIO):
    def __init__(self, method: Callable[..., requests.Response],
                 uri: D.URI, stream: bool = True) -> None:
        self._method = method
        self._uri = uri
        self._stream = stream
        self._reader: Optional[IO[bytes]] = None

    def read(self, size: Optional[int] = -1) -> bytes:
        if self._reader is None:
            response = self._method(str(self._uri), stream=self._stream)
            print(response.content)
            self._reader = ReadWrapper(response.iter_content)
        if size is None:
            size = -1
        return self._reader.read(size)
