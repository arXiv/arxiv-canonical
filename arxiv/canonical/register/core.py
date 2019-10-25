"""
Core structures of the canonical record.


"""
import io
import os
from collections import abc, defaultdict
from datetime import date, datetime
from functools import partial
from itertools import groupby
from operator import attrgetter
from typing import (Dict, Tuple, Iterator, List,
                    Generic, TypeVar, MutableMapping, Optional, Iterable,
                    Callable, Any, Type, Union, Set, IO, Sequence, cast)

from typing_extensions import Literal, Protocol

from ..core import ICanonicalSource, dereference
from .. import domain as D
from .. import record as R
from .. import integrity as I
from ..manifest import Manifest, ManifestEntry, make_empty_manifest

from .util import LazyMap, LazyMapView

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class IStorableEntry(Protocol):
    """Minimal interface for a storable object."""

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
        """Store an integrity manifest."""

    def load_manifest(self, key: D.Key) -> Manifest:  # pylint: disable=unused-argument; this is a stub.
        """Load an integrity manifest."""


_I = TypeVar('_I', I.IntegrityEntry, I.IntegrityMetadata, I.IntegrityListing,
             covariant=True)


class ICanonicalStorage(ICanonicalSource, IManifestStorage, Protocol):
    """Storage protocol for the canonical R."""

    def list_subkeys(self, key: D.URI) -> List[str]:  # pylint: disable=unused-argument; this is a stub.
        """List all of the subkeys for ``key`` in the record."""

    def store_entry(self, ri: IStorableEntry) -> None:  # pylint: disable=unused-argument; this is a stub.
        """
        Store an entry in the record.

        This method MUST decompress the content of the entry if it is gzipped
        (as is sometimes the case in the classic system) and update the
        ``CanonicalFile`` (``ri.record.stream.domain``).
        """


_Name = TypeVar('_Name')
_Domain = TypeVar('_Domain')
_Record = TypeVar('_Record', bound=Union[R.RecordBase, R.RecordEntry])
_Integrity = TypeVar('_Integrity', bound=I.IntegrityBase)
_Member = TypeVar('_Member', bound=Optional['Base'])
_MemberName = TypeVar('_MemberName')
_Self = TypeVar('_Self', bound='Base')


class Base(Generic[_Name, _Domain, _Record, _Integrity, _MemberName, _Member]):
    """
    Generic base class for all register classes.

    This defines the abstract structure of a register class, specifically that
    instances of a register class are composed of a domain object, a record
    object, an integrity object, and a set of members. This allows us to
    define register classes that align domain, record, and integrity classes
    at a specific level of the record hierarchy.
    """

    domain: _Domain
    """The domain object on a register instance."""

    domain_type: Type[_Domain]
    """The type of the domain object on a register instance."""

    record: _Record
    """The record object on a register instance."""

    record_type: Type[_Record]
    """The type of the record object on a register instance."""

    integrity: _Integrity
    """The integrity object on a register instance."""

    integrity_type: Type[_Integrity]
    """The type of the integrity object on a register instance."""

    member_type: Type[_Member]
    """The type of members contained by an instance of a register class."""

    def __init__(
            self,
            name: _Name,
            domain: _Domain,
            record: _Record,
            integrity: _Integrity,
            members: Optional[MutableMapping[_MemberName, _Member]] = None
        ) -> None:
        """Set public and private attributes."""
        self.domain = domain
        self.record = record
        self.integrity = integrity
        self.name = name
        self._members = members

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[_MemberName]:
        """
        Get the name of a member that contains an event.

        This method routes events to members when new events are added to the
        record.
        """
        raise NotImplementedError(f'Must be implemented by child ({cls}),'
                                  ' if supported')

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> _MemberName:
        """Get the name of the member corresponding to a manifest key."""
        raise NotImplementedError('Must be implemented by a child class')

    @classmethod
    def _get_members(cls, s: ICanonicalStorage,
                     sources: Sequence[ICanonicalSource],
                     manifest: Manifest) \
            -> MutableMapping[_MemberName, _Member]:
        """Generate a member mapping from a :class:`Manifest`."""
        return LazyMap([cls._manifest_to_member_name(entry['key'])
                        for entry in manifest['entries']],
                       partial(cls.member_type.load, s, sources))

    @classmethod
    def load(cls: Type[_Self], s: ICanonicalStorage,
             sources: Sequence[ICanonicalSource], name: _Name,
             checksum: Optional[str] = None) -> _Self:
        """Load an instance of the register class from storage."""
        manifest_key = cls.record_type.make_manifest_key(name)
        try:
            manifest = s.load_manifest(manifest_key)
        except Exception:    # TODO: need a storage exception here.
            manifest = make_empty_manifest()

        members = cls._get_members(s, sources, manifest)
        d = cls.domain_type(name, LazyMapView(members, _get_domain))
        r = cls.record_type(name, LazyMapView(members, _get_record), d)
        i = cls.integrity_type(
            name,
            record=r,
            members=LazyMapView(members, _get_integrity),
            manifest=manifest,
            checksum=checksum
        )
        return cls(name, domain=d, record=r, integrity=i, members=members)

    @classmethod
    def _load_content(cls: Type[_Self], s: ICanonicalStorage,
                      sources: Sequence[ICanonicalSource],
                      identifier: D.VersionedIdentifier,
                      filename: str) -> IO[bytes]:
        return dereference(sources,
                         R.RecordVersion.make_key(identifier, filename))

    @property
    def members(self) -> MutableMapping[_MemberName, _Member]:
        """Accessor for the members of a register instance."""
        assert self._members is not None
        return self._members

    @property
    def number_of_events(self) -> int:
        """Number of events contained within a register instance."""
        return self.integrity.manifest.get('number_of_events', -1)

    @property
    def number_of_versions(self) -> int:
        """Number of e-print versions contained within a register instance."""
        return self.integrity.manifest.get('number_of_versions', -1)

    def add_events(self, s: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   *events: D.Event) -> None:
        """Add events to this register."""
        self.save_members(
            s,
            self._add_events(s, sources, events, self._member_name)
        )
        assert self.integrity.manifest is not None
        self.integrity.update_checksum()

    def iter_members(self) -> Iterable[_Member]:
        """Get an iterator over members in this register."""
        assert self.members is not None
        return (self.members[name] for name in self.members)

    def save(self, s: ICanonicalStorage) -> str:
        """Store changes to the integrity manifest for this register."""
        s.store_manifest(self.record.make_manifest_key(self.name),
                         self.integrity.manifest)
        return self.integrity.checksum

    def save_members(self, s: ICanonicalStorage,
                     members: Iterable[_Member]) -> None:
        """Save members that have changed, and update our manifest."""
        for member in members:
            checksum = member.save(s)
            assert checksum is not None
            self.integrity.update_or_extend_manifest(member, checksum)


    def _add_events(self, s: ICanonicalStorage,
                    sources: Sequence[ICanonicalSource],
                    events: Iterable[D.Event],
                    fkey: Callable[[D.Event], Iterable[_MemberName]]) \
            -> Iterable[_Member]:
        assert self.members is not None
        altered = set()
        grouped: Dict[_MemberName, List[D.Event]] = defaultdict(list)
        for event in events:
            for name in fkey(event):
                grouped[name].append(event)
        for name, m_events in grouped.items():
            member = self.members[name]
            member.add_events(s, sources, *m_events)
            altered.add(member)
        return iter(altered)


def _get_domain(register: Base[_Name,
                               _Domain,
                               _Record,
                               _Integrity,
                               _MemberName,
                               _Member]) -> _Domain:
    return register.domain


def _get_record(register: Base[_Name,
                               _Domain,
                               _Record,
                               _Integrity,
                               _MemberName,
                               _Member]) -> _Record:
    return register.record


def _get_integrity(register: Base[_Name,
                                  _Domain,
                                  _Record,
                                  _Integrity,
                                  _MemberName,
                                  _Member]) -> _Integrity:
    return register.integrity





