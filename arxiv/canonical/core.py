"""Core interfaces for the canonical record."""

import io
import datetime
from typing import Any, Callable, Dict, IO, Iterable, List, Sequence, Tuple, Type, \
    TypeVar, Union

from typing_extensions import Protocol

from . import domain as D
from . import integrity as I
from . import record as R
from .manifest import Manifest


class IEventStream(Protocol):
    """Interface for the canonical event stream."""

    def emit(self, event: D.Event) -> None:
        """
        Emit an :class:`Event` on the stream.

        Parameters
        ----------
        event : :class:`Event`

        """

    def listen(self, on_event: Callable[[D.Event], None]) -> None:
        """
        Listen for :class:`Event`s on the stream.

        Parameters
        ----------
        on_event : callable
            This object will be called for every event that is received. It
            should accept a single argument, an :class:`.Event`, and is
            expected to return ``None``.

        """


class ICanonicalSource(Protocol):
    """Interface for source services, used to dereference URIs."""

    def can_resolve(self, uri: D.URI) -> bool:
        """
        Indicate whether or not the implementation can resolve an URI.

        Parameters
        ----------
        uri : :class:`.D.URI`

        Returns
        -------
        bool

        """

    def load(self, key: D.URI) -> IO[bytes]:  # pylint: disable=unused-argument; this is a stub.
        """
        Make an IO that waits to load from the record until it is read().

        Parameters
        ----------
        key : :class:`D.URI`

        Returns
        -------
        IO
            Yields bytes when read. This may be a lazy IO object, so that
            reading is deferred until the latest possible time.

        """


class IStorableEntry(Protocol):
    """
    Minimal interface for a bitstream interface that can be stored.

    Services that implement :class:`.ICanonicalStorage` can assume that the
    attributes of this interface are available on objects passed for storing.
    """

    name: str
    """Name of the entry."""  # pylint: disable=pointless-string-statement; this is a docstring.

    @property
    def checksum(self) -> str:
        """URL-safe b64-encoded md5 hash."""

    @property
    def record(self) -> R.RecordEntry:
        """Reference to a :class:`.RecordEntry`."""

    def update_checksum(self) -> None:
        """Update the integrity checksum for this entry."""


class IManifestStorage(Protocol):
    """
    Manifest protocol.

    This could conceivably be stored separately from the canonical record
    content, so it is defined separately.
    """

    def store_manifest(self, key: D.Key, manifest: Manifest) -> None:  # pylint: disable=unused-argument; this is a stub.
        """
        Store an integrity manifest.

        Parameters
        ----------
        key : :class:`.Key`
            Key used to identify ``manifest`` in storage.
        manifest : :class:`.Manifest`
            The manifest record to store.

        """

    def load_manifest(self, key: D.Key) -> Manifest:  # pylint: disable=unused-argument; this is a stub.
        """
        Load an integrity manifest.

        Parameters
        ----------
        key : :class:`.Key`
            Key used to identify ``manifest`` in storage.

        Returns
        -------
        :class:`.Manifest`

        """


_I = TypeVar('_I', I.IntegrityEntry, I.IntegrityMetadata, I.IntegrityListing,
             covariant=True)


class ICanonicalStorage(ICanonicalSource, IManifestStorage, Protocol):
    """Interface for services that store the canonical record."""

    def list_subkeys(self, key: D.URI) -> List[str]:  # pylint: disable=unused-argument; this is a stub.
        """
        List all of the subkeys (direct descendants) of ``key`` in the record.

        Parameters
        ----------
        key : :class:`.URI`

        Returns
        -------
        list
            Items are the relative names of the descendants of ``key``. For
            filesystem-based storage, this may be equivalent to ``os.listdir``.

        """

    def store_entry(self, ri: IStorableEntry) -> None:  # pylint: disable=unused-argument; this is a stub.
        """
        Store a bitstream entry in the record.

        This method MUST decompress the content of the entry if it is gzipped
        (as is sometimes the case in the classic system) and update the
        ``CanonicalFile`` (``ri.record.stream.domain``).

        Parameters
        ----------
        ri : :class:`.IStorableEntry`
            A storable bitstream.

        """

    def load_entry(self, key: D.URI) -> Tuple[R.RecordStream, str]:
        """
        Load a bitstream entry.

        Parameters
        ----------
        key : :class:`.URI`
            Key that identifies the bitsream in the record.

        Returns
        -------
        :class:`.RecordStream`
            The bitstream resource.
        str
            Checksum of the bitstream (URL-safe base64-encoded md5 hash).ß

        """


Year = int
"""Years are represented as four-digit integers."""

Month = int
"""Months are represented as integers."""

YearMonth = Tuple[Year, Month]
"""A month in a particular year is represented as a 2-tuple of integers."""

Selector = Union[Year, YearMonth, datetime.date]
"""A selector can refer to a year, month, or a specific date."""

_ID = Union[D.VersionedIdentifier, D.Identifier]


class IRegisterAPI(Protocol):
    """Interface for the canonical register API."""

    def add_events(self, *events: D.Event) -> None:
        """Add new events to the register."""

    def load_version(self, identifier: D.VersionedIdentifier) -> D.Version:  # pylint: disable=unused-argument; this is a stub.
        """Load an e-print :class:`.Version` from the record."""

    def load_eprint(self, identifier: D.Identifier) -> D.EPrint:  # pylint: disable=unused-argument; this is a stub.
        """Load an :class:`.EPrint` from the record."""

    def load_history(self, identifier: _ID) -> Iterable[D.EventSummary]:  # pylint: disable=unused-argument; this is a stub.
        """Load the event history of an :class:`.EPrint`."""

    def load_event(self, identifier: str) -> D.Event:  # pylint: disable=unused-argument; this is a stub.
        """Load an :class:`.Event` by identifier."""

    def load_events(self, selector: Selector) -> Tuple[Iterable[D.Event], int]:  # pylint: disable=unused-argument; this is a stub.
        """Load all :class:`.Event`s for a day, month, or year."""

    def load_listing(self, date: datetime.date,   # pylint: disable=unused-argument; this is a stub.
                     shard: str = D.Event.get_default_shard()) -> D.Listing:  # pylint: disable=no-member, unused-argument
        """Load a :class:`.Listing` for a particulate date."""


# TODO: consider a semantically more meaningful exception for failure to
# dereference the URI.
def dereference(sources: Sequence[ICanonicalSource], uri: D.URI) -> IO[bytes]:
    """
    Dereference an URI using a set of available sources.

    Sources are checked one at a time for ability to resolve the URI. When one
    is found, the URI is loaded.

    Parameters
    ----------
    sources : sequence
        Items are content sources that should conform to
        :class:`.ICanonicalSource`. They will be tried in the order provided.
    uri : :class:`.URI`
        URI to dereference.

    Returns
    -------
    io
        BytesIO object.

    Raises
    ------
    :class:`RuntimeError`
        Raised when the URI cannot be resolved.

    """
    for source in sources:
        if source.can_resolve(uri):
            return source.load(uri)
    raise RuntimeError(f'Cannot resolve URI: {uri}')