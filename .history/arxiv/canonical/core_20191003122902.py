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


def uri_to_io(sources: Sequence[ICanonicalSource], uri: D.URI) -> IO[bytes]:
    print('uri_to_io::', uri)
    for source in sources:
        print('source: ', source)
        if source.can_resolve(uri):
            return source.load_deferred(uri)
    raise RuntimeError('Cannot resolve URI')


def partial_version_uri_to_io(sources: Sequence[ICanonicalSource]) \
        -> Callable[[Type[D.Version], D.Version], D.Version]:
    def _uri_to_io(_: Type[D.Version], obj: D.Version) -> D.Version:
        if isinstance(obj.source.content, D.URI):
            obj.source.content = uri_to_io(sources, obj.source.content)
        if isinstance(obj.render.content, D.URI):
            obj.render.content = uri_to_io(sources, obj.render.content)
        return obj
    return _uri_to_io


def io_to_uri(obj: D.Version, data: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(obj.source.content, io.BytesIO):
        data['source']['content'] = R.RecordVersion.make_key(
            obj.identifier,
            obj.source.filename
        )
    if isinstance(obj.render.content, io.BytesIO):
        data['render']['content'] = R.RecordVersion.make_key(
            obj.identifier,
            obj.render.filename
        )
    return data