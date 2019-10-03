import io
from typing import Any, Callable, Dict, IO, Sequence, Tuple, Type

from typing_extensions import Protocol

from . import domain as D
from . import record as R


class ICanonicalSource(Protocol):

    def can_resolve(self, uri: D.URI) -> bool:
        ...

    def load_deferred(self, key: D.URI) -> IO[bytes]:  # pylint: disable=unused-argument; this is a stub.
        """Make an IO that waits to load from the record until it is read()."""
        ...  # pylint: disable=pointless-statement; this is a stub.

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        ...  # pylint: disable=pointless-statement; this is a stub.


def dereference(sources: Sequence[ICanonicalSource], uri: D.URI) -> IO[bytes]:
    for source in sources:
        if source.can_resolve(uri):
            return source.load_deferred(uri)
    raise RuntimeError('Cannot resolve URI')

