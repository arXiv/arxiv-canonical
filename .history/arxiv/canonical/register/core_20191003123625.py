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
                    Callable, Any, Type, Union, Set, IO, Sequence, overload,
                    cast)

from typing_extensions import Literal, Protocol

from ..core import ICanonicalSource, partial_version_uri_to_io, io_to_uri, uri_to_io
from .. import domain as D
from .. import record as R
from .. import integrity as I

from .util import LazyMap, LazyMapView

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class ConsistencyError(Exception):
    """Operation was attempted that would violate consistency of the record."""


class NoSuchResource(Exception):
    """Operation was attempted on a non-existant resource."""


class IStorableEntry(Protocol):
    """Minimal interface for a storable object."""

    name: str
    """Name of the entry."""  # pylint: disable=pointless-string-statement; this is a docstring.

    @property
    def record(self) -> R.RecordEntry:
        """Reference to a :class:`.RecordEntry`."""
        ...  # pylint: disable=pointless-statement; this is a stub.


class IManifestStorage(Protocol):
    """
    I.Manifest s protocol.

    This could conceivably be stored separately from the canonical record
    content, so it is defined separately.
    """

    def store_manifest(self, key: D.Key, manifest: I.Manifest) -> None:  # pylint: disable=unused-argument; this is a stub.
        """Store an integrity manifest."""
        ...  # pylint: disable=pointless-statement; this is a stub.

    def load_manifest(self, key: D.Key) -> I.Manifest:  # pylint: disable=unused-argument; this is a stub.
        """Load an integrity manifest."""
        ...  # pylint: disable=pointless-statement; this is a stub.


_I = TypeVar('_I', I.IntegrityEntry, I.IntegrityMetadata, I.IntegrityListing,
             covariant=True)


class ICanonicalStorage(ICanonicalSource, IManifestStorage, Protocol):
    """Storage protocol for the canonical R."""

    def list_subkeys(self, key: D.URI) -> List[str]:  # pylint: disable=unused-argument; this is a stub.
        """List all of the subkeys for ``key`` in the record."""
        ...  # pylint: disable=pointless-statement; this is a stub.

    def store_entry(self, ri: IStorableEntry) -> None:  # pylint: disable=unused-argument; this is a stub.
        """Store an entry in the record."""
        ...  # pylint: disable=pointless-statement; this is a stub.


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
                     manifest: I.Manifest) \
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
            manifest = I.make_empty_manifest()

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
        return uri_to_io(sources,
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


class RegisterFile(Base[str,
                        D.CanonicalFile,
                        R.RecordFile,
                        I.IntegrityEntry,
                        None,
                        None]):

    domain_type = D.CanonicalFile
    record_type = R.RecordFile
    integrity_type = I.IntegrityEntry
    member_type = type(None)

    def save(self, s: ICanonicalStorage) -> str:
        """
        Save this file.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        s.store_entry(self.integrity)
        self.integrity.update_checksum()
        return self.integrity.checksum

    def delete(self, s: ICanonicalStorage) -> None:
        raise NotImplementedError('not yet; do this please')


class RegisterMetadata(Base[str,
                            D.Version,
                            R.RecordMetadata,
                            I.IntegrityMetadata,
                            None,
                            None]):

    domain_type = D.Version
    record_type = R.RecordMetadata
    integrity_type = I.IntegrityMetadata
    member_type = type(None)

    def save(self, s: ICanonicalStorage) -> str:
        """
        Save this file.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        s.store_entry(self.integrity)
        self.integrity.update_checksum()
        return self.integrity.checksum

    def delete(self, s: ICanonicalStorage) -> None:
        raise NotImplementedError('not yet; do this please')


class RegisterListing(Base[D.ListingIdentifier,
                           D.Listing,
                           R.RecordListing,
                           I.IntegrityListing,
                           None,
                           None]):

    domain_type = D.Listing
    record_type = R.RecordListing
    integrity_type = I.IntegrityListing
    member_type = type(None)

    @classmethod
    def create(cls, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               d: D.Listing) -> 'RegisterListing':
        # callbacks = [partial_uri_to_io(sources)]
        r = R.RecordListing.from_domain(d, callbacks=[])
        i = I.IntegrityListing.from_record(r)
        s.store_entry(i)
        return cls(d.identifier, domain=d, record=r, integrity=i)

    @classmethod
    def load(cls: Type[_Self], s: ICanonicalStorage,
             sources: Sequence[ICanonicalSource],
             identifier: D.ListingIdentifier,
             checksum: Optional[str] = None) -> _Self:

        callbacks = [partial_version_uri_to_io(sources)]
        try:
            key = R.RecordListing.make_key(identifier)
            stream, _checksum = s.load_entry(key)

            d = R.RecordListing.to_domain(stream, callbacks=callbacks)
            r = R.RecordListing(key=key, stream=stream, domain=d)
            if checksum is not None:
                assert checksum == _checksum
            i = I.IntegrityListing.from_record(r, checksum=_checksum,
                                               calculate_new_checksum=False)

            # d = _i.record.to_domain(partial(cls._load_content, s))
            # r = R.RecordListing.from_domain(d)
            #     domain=d,
            #     key=_i.record.key,
            #     content=_i.record.content,
            #     content_type=_i.record.content_type,
            #     size_bytes=_i.record.size_bytes
            # )
            # i = I.IntegrityListing.from_record(
            #     r,
            #     checksum=checksum,
            #     calculate_new_checksum=False
            # )
        except Exception:
            d = D.Listing(identifier, events=[])
            r = R.RecordListing.from_domain(d, callbacks=[])
            i = I.IntegrityListing.from_record(r)
        return cls(identifier, domain=d, integrity=i, record=r)

    @property
    def number_of_events(self) -> int:
        return self.domain.number_of_events

    @property
    def number_of_versions(self) -> int:
        return self.domain.number_of_versions

    def add_events(self, _: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   *events: D.Event) -> None:
        """
        Add events to the terminal listing R.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        N = len(events)
        for i, event in enumerate(events):
            self.domain.events.insert(N + i, event)
        callbacks = [partial_version_uri_to_io(sources)]
        self.record = R.RecordListing.from_domain(self.domain,
                                                  callbacks=[])
        self.integrity = I.IntegrityListing.from_record(self.record)

    def save(self, s: ICanonicalStorage) -> str:
        """
        Save this file.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        s.store_entry(self.integrity)
        self.integrity.update_checksum()
        return self.integrity.checksum

    def delete(self, s: ICanonicalStorage) -> None:
        raise NotImplementedError('not yet; do this please')


class RegisterVersion(Base[D.VersionedIdentifier,
                           D.Version,
                           R.RecordVersion,
                           I.IntegrityVersion,
                           str,
                           RegisterFile]):
    domain_type = D.Version
    record_type = R.RecordVersion
    integrity_type = I.IntegrityVersion
    member_type = RegisterFile

    @classmethod
    def create(cls, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               d: D.Version, save_members: bool = True) -> 'RegisterVersion':
        r = R.RecordVersion.from_domain(d, partial(uri_to_io, sources), callbacks=[])  # <- need to dereference URIs here
        i = I.IntegrityVersion.from_record(r)
        members = {}
        for i_member in i.iter_members():
            if isinstance(i_member.record, R.RecordFile):
                assert isinstance(i_member.record.domain, D.CanonicalFile)
                member = RegisterFile(i_member.name,
                                      domain=i_member.record.domain,
                                      record=i_member.record,
                                      integrity=i_member)
            elif isinstance(i_member.record, R.RecordMetadata):
                assert isinstance(i_member.record.domain, D.Version)
                member = RegisterMetadata(i_member.name,
                                          domain=i_member.record.domain,
                                          record=i_member.record,
                                          integrity=i_member)
            if save_members:
                member.save(s)
            members[member.name] = member
        return cls(r.name, domain=d, record=r, integrity=i, members=members)

    @classmethod
    def load(cls: Type[_Self], s: ICanonicalStorage,
             sources: Sequence[ICanonicalSource],
             identifier: D.VersionedIdentifier,
             checksum: Optional[str] = None) -> _Self:
        """
        Load an e-print :class:`.Version` from s.

        This method is overridden since it uses a different member mapping
        struct than higher-level collection types.
        """
        # All of the data needed to reconstitute the Version is in the metadata
        # record.
        key = R.RecordMetadata.make_key(identifier)
        stream, _ = s.load_entry(key)
        # callbacks = [partial_version_uri_to_io(sources)]
        d = R.RecordMetadata.to_domain(stream, callbacks=[])   # self.load_deferred
        _r = R.RecordMetadata(key=key, stream=stream, domain=d)

        assert d.source is not None and d.render is not None
        # assert d.source.content is not None and d.render.content is not None
        manifest = s.load_manifest(R.RecordVersion.make_manifest_key(identifier))
        r = R.RecordVersion.from_domain(d, partial(uri_to_io, sources), metadata=_r, callbacks=[])
        i = I.IntegrityVersion.from_record(
            r,
            checksum=checksum,
            calculate_new_checksum=False,
            manifest=manifest
        )
        return cls(date, domain=d, record=r, integrity=i)

    @property
    def member_names(self) -> Set[str]:
        assert self.members is not None
        return set([name for name in self.members])

    @property
    def number_of_events(self) -> int:
        return 0

    @property
    def number_of_versions(self) -> int:
        return 1

    def update(self, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               version: D.Version) -> None:
        """
        Update a version in place.

        Removes any members (files) not in the passed ``Version``, and retains
        and ignores members without any content (assumes that this is a partial
        update). Saves any new/changed members, and updates the manifest.
        """
        new_version = self.create(s, sources, version, save_members=False)
        assert self.members is not None and new_version.members is not None
        to_remove = self.member_names - new_version.member_names

        to_add = [name for name in new_version.members
                  # Ignore any members without content, as this may be a
                  # partial update only.
                  if new_version.members[name].domain.content is not None
                    # Select members not already present, or...
                    and (name not in self.members
                         # ...that appear to have changed.
                         or self.members[name].integrity.checksum
                            != new_version.members[name].integrity.checksum)]
        for name in to_remove:
            self.members[name].delete(s)
            del self.members[name]
        altered = set()
        for name in to_add:
            self.members[name] = new_version.members[name]
            altered.add(self.members[name])
        self.save_members(s, altered)   # Updates our manifest.


class RegisterEPrint(Base[D.Identifier,
                          D.EPrint,
                          R.RecordEPrint,
                          I.IntegrityEPrint,
                          D.VersionedIdentifier,
                          RegisterVersion]):
    domain_type = D.EPrint
    record_type = R.RecordEPrint
    integrity_type = I.IntegrityEPrint
    member_type = RegisterVersion

    @classmethod
    def _member_name(cls, event: D.Event) \
            -> Iterable[D.VersionedIdentifier]:
        return [event.version.identifier]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> D.VersionedIdentifier:
        return D.VersionedIdentifier(key)

    def _add_versions(self, s: ICanonicalStorage,
                      sources: Sequence[ICanonicalSource],
                      versions: Iterable[D.Version],
                      fkey: Callable[[D.Version], Any]) \
            -> Iterable[RegisterVersion]:
        assert self.members is not None
        altered = set()
        for version in versions:
            key = fkey(version)
            if key in self.members:
                raise ConsistencyError('Version already exists')
            member = self.member_type.create(s, sources, version)
            self.members[key] = member
            altered.add(member)
        return iter(altered)

    def add_event_new(self, s: ICanonicalStorage,
                      sources: Sequence[ICanonicalSource],
                      event: D.Event) -> List[RegisterVersion]:
        assert self.members is not None
        altered: List[RegisterVersion] = []
        for key in self._member_name(event):
            if key in self.members:
                raise ConsistencyError('Version already exists')
            self.members[key] \
                = self.member_type.create(s, sources, event.version)
            altered.append(self.members[key])
        return altered

    def add_event_update(self, s: ICanonicalStorage,
                         sources: Sequence[ICanonicalSource],
                         event: D.Event) -> List[RegisterVersion]:
        assert self.members is not None
        altered: List[RegisterVersion] = []
        for key in self._member_name(event):
            if key not in self.members:
                raise ConsistencyError(f'No such version: {event.identifier}')
            self.members[key].update(s, sources, event.version)
            altered.append(self.members[key])
        return altered

    def add_event_update_metadata(self, s: ICanonicalStorage,
                                  sources: Sequence[ICanonicalSource],
                                  event: D.Event) -> List[RegisterVersion]:
        if event.version.source is not None:
            assert event.version.source.content is None
        if event.version.render is not None:
            assert event.version.render.content is None
        return self.add_event_update(s, sources, event)

    def add_event_replace(self, s: ICanonicalStorage,
                          sources: Sequence[ICanonicalSource],
                          event: D.Event) -> List[RegisterVersion]:
        return self.add_event_new(s, sources, event)

    def add_event_cross(self, s: ICanonicalStorage,
                        sources: Sequence[ICanonicalSource],
                        event: D.Event) -> List[RegisterVersion]:
        return self.add_event_update_metadata(s, sources, event)

    def add_event_migrate(self, s: ICanonicalStorage,
                          sources: Sequence[ICanonicalSource],
                          event: D.Event) -> List[RegisterVersion]:
        return self.add_event_update(s, sources, event)

    def add_event_migrate_metadata(self, s: ICanonicalStorage,
                                   sources: Sequence[ICanonicalSource],
                                   event: D.Event) -> List[RegisterVersion]:
        return self.add_event_update_metadata(s, sources, event)

    def _add_events(self, s: ICanonicalStorage,
                    sources: Sequence[ICanonicalSource],
                    events: Iterable[D.Event],
                    _: Callable) -> Iterable[RegisterVersion]:
        added: Set[RegisterVersion] = set()
        for event in events:
            adder = getattr(self, f'add_event_{event.event_type.value}', None)
            assert adder is not None
            added |= set(adder(s, sources, event))
        return added


class RegisterDay(Base[date,
                       D.EPrintDay,
                       R.RecordDay,
                       I.IntegrityDay,
                       D.Identifier,
                       RegisterEPrint]):
    domain_type = D.EPrintDay
    record_type = R.RecordDay
    integrity_type = I.IntegrityDay
    member_type = RegisterEPrint

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[D.Identifier]:
        return [event.version.identifier.arxiv_id]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> D.Identifier:
        return D.Identifier(key)


class RegisterMonth(Base[YearMonth,
                         D.EPrintMonth,
                         R.RecordMonth,
                         I.IntegrityMonth,
                         date,
                         RegisterDay]):
    domain_type = D.EPrintMonth
    record_type = R.RecordMonth
    integrity_type = I.IntegrityMonth
    member_type = RegisterDay

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[date]:
        return [event.version.announced_date_first]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> date:
        return datetime.strptime(key, '%Y-%m-%d').date()


class RegisterYear(Base[Year,
                        D.EPrintYear,
                        R.RecordYear,
                        I.IntegrityYear,
                        YearMonth,
                        RegisterMonth]):
    domain_type = D.EPrintYear
    record_type = R.RecordYear
    integrity_type = I.IntegrityYear
    member_type = RegisterMonth

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[YearMonth]:
        return [(event.version.identifier.year,
                 event.version.identifier.month)]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> YearMonth:
        year_part, month_part = key.split('-', 1)
        return int(year_part), int(month_part)


class RegisterEPrints(Base[str,
                           D.AllEPrints,
                           R.RecordEPrints,
                           I.IntegrityEPrints,
                           Year,
                           RegisterYear]):
    domain_type = D.AllEPrints
    record_type = R.RecordEPrints
    integrity_type = I.IntegrityEPrints
    member_type = RegisterYear

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[Year]:
        return [event.version.identifier.year]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> Year:
        return int(key)


class RegisterListingDay(Base[date,
                              D.ListingDay,
                              R.RecordListingDay,
                              I.IntegrityListingDay,
                              D.ListingIdentifier,
                              RegisterListing]):
    domain_type = D.ListingDay
    record_type = R.RecordListingDay
    integrity_type = I.IntegrityListingDay
    member_type = RegisterListing

    @classmethod
    def _member_name(cls, event: D.Event) \
            -> Iterable[D.ListingIdentifier]:
        return [D.ListingIdentifier.from_parts(event.event_date.date(),
                                                    event.event_id.shard)]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> D.ListingIdentifier:
        # return ListingIdentifier(key)
        base, term = os.path.split(key)
        term, _ = os.path.splitext(term)
        y, m, d, shrd = term.split('-', 3)
        return D.ListingIdentifier.from_parts(date(int(y), int(m),
                                                   int(d)), shrd)

    @classmethod
    def load_event(cls, s: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   identifier: D.EventIdentifier) -> D.Event:
        listing = cls.load(s, sources, identifier.event_date)
        for member in listing.members:
            for event in listing.members[member].domain.events:
                if event.event_id == identifier:
                    return event
        raise NoSuchResource(f'No such event: {identifier}')

    def add_listing(self, s: ICanonicalStorage,
                    sources: Sequence[ICanonicalSource],
                    d: D.Listing) -> None:
        assert self.members is not None
        member = RegisterListing.create(s, sources, d)
        self.members[member.domain.identifier] = member
        self.integrity.extend_manifest(member.integrity)

    # @classmethod
    # def load(cls: Type[_Self], s: ICanonicalStorage,
    #          date: date, checksum: Optional[str] = None) -> _Self:
    #     """
    #     Load a listing from s.

    #     This method is overridden since it uses a different member mapping
    #     struct than higher-level collection types.
    #     """
    #     manifest_key = cls.record_type.make_manifest_key(date)
    #     try:
    #         manifest = s.load_manifest(manifest_key)
    #         members: Dict[ListingIdentifier, RegisterListing]
    #         # Load each of the listing files.
    #         for manifest_name, manifest_entry in manifest['entries'].items():
    #             member_name = cls._manifest_to_member_name(manifest_name)
    #             integrity = s.load_entry(R.RecordListing.make_key(member_name))
    #             domain = ListingSerializer.to_domain(I.record)
    #             I.record.domain = domain

    #             members[member_name] = RegisterListing(
    #                 member_name,
    #                 domain=domain,
    #                 integrity=integrity,
    #                 record=I.record
    #             )


    #         # Listing already exists.
    #         listing = s.load_entry(cls.record_type.make_key(date))
    #         integrity = I.IntegrityListing(
    #             date,
    #             members=I.IntegrityEntryMembers(listing=listing),
    #             manifest=manifest,
    #             checksum=checksum
    #         )
    #         record = R.RecordListing(
    #             date,
    #             members=R.RecordEntryMembers(listing=listing.record)
    #         )
    #         domain = ListingSerializer.to_domain(record)
    #     except Exception as e:
    #         print(e)
    #         # New listing.
    #         print('new listing', cls)
    #         manifest = I.Manifest(entries=[], number_of_entries=0,
    #                             number_of_events=0,
    #                             number_of_versions=0)
    #         domain = Listing(date=date, events=[])
    #         record = ListingSerializer.serialize(domain)
    #         integrity = I.IntegrityListing.from_record(record)
    #     return cls(date, domain=domain, record=record, integrity=integrity)


class RegisterListingMonth(Base[YearMonth,
                                D.ListingMonth,
                                R.RecordListingMonth,
                                I.IntegrityListingMonth,
                                date,
                                RegisterListingDay]):

    domain_type = D.ListingMonth
    record_type = R.RecordListingMonth
    integrity_type = I.IntegrityListingMonth
    member_type = RegisterListingDay

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[date]:
        return [event.event_date.date()]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> date:
        return datetime.strptime(key, '%Y-%m-%d').date()


class RegisterListingYear(Base[Year,
                               D.ListingYear,
                               R.RecordListingYear,
                               I.IntegrityListingYear,
                               YearMonth,
                               RegisterListingMonth]):
    domain_type = D.ListingYear
    record_type = R.RecordListingYear
    integrity_type = I.IntegrityListingYear
    member_type = RegisterListingMonth

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[YearMonth]:
        return [(event.event_date.year, event.event_date.month)]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> YearMonth:
        year_part, month_part = key.split('-', 1)
        return int(year_part), int(month_part)


class RegisterListings(Base[str,
                            D.AllListings,
                            R.RecordListings,
                            I.IntegrityListings,
                            Year,
                            RegisterListingYear]):
    domain_type = D.AllListings
    record_type = R.RecordListings
    integrity_type = I.IntegrityListings
    member_type = RegisterListingYear

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[Year]:
        return [event.event_date.year]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> Year:
        return int(key)


listings_key = Literal['listings']
eprints_key = Literal['eprints']
_TopLevelNames = Union[listings_key, eprints_key]
_TopLevelMembers = Union[RegisterListings, RegisterEPrints]


class _TopMapping(abc.MutableMapping):
    def __init__(self, listings: RegisterListings,
                 eprints: RegisterEPrints) -> None:
        """Initilize with listings and eprints registers."""
        self.eprints = eprints
        self.listings = listings

    @overload
    def __getitem__(self, obj: listings_key) -> RegisterListings: ...
    @overload
    def __getitem__(self, obj: eprints_key) -> RegisterEPrints: ...  # pylint: disable=function-redefined
    def __getitem__(self, obj: Any) -> Any:  # pylint: disable=function-redefined
        if obj == 'eprints':
            return self.eprints
        if obj == 'listings':
            return self.listings
        raise KeyError('No such resource')

    def __delitem__(self, obj: Any) -> None:
        raise NotImplementedError('Does not support deletion')

    def __setitem__(self, obj: Any, value: Any) -> None:
        if obj == 'eprints' and isinstance(value, RegisterEPrints):
            self.eprints = value
        if obj == 'listings' and isinstance(value, RegisterListings):
            self.listings = value
        raise ValueError('Not supported')

    def __iter__(self) -> Iterator[Any]:
        return iter([self.eprints, self.listings])

    def __len__(self) -> int:
        return 2


class Register(Base[str,
                    D.Canon,
                    R.Record,
                    I.Integrity,
                    _TopLevelNames,
                    _TopLevelMembers]):
    domain_type = D.Canon
    record_type = R.Record
    integrity_type = I.Integrity
    member_type = _TopLevelMembers  # type: ignore

    @classmethod
    def _member_name(cls, _: D.Event) -> Iterable[_TopLevelNames]:
        return ['listings', 'eprints']

    @classmethod
    def _get_members(cls, s: ICanonicalStorage,
                     sources: Sequence[ICanonicalSource],
                     _: I.Manifest) -> _TopMapping:
        return _TopMapping(RegisterListings.load(s, sources, 'listings'),
                           RegisterEPrints.load(s, sources, 'eprints'))

    def add_events(self, s: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   *events: D.Event) -> None:
        """Add events to this register."""
        for event in events:
            event.version.events.append(event.summary)
        super(Register, self).add_events(s, sources, *events)

