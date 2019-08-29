import collections
import datetime
from functools import partial
from itertools import groupby
from operator import attrgetter
from typing import (Dict, Tuple, Iterator, List, KeysView, NamedTuple,
                    Generic, TypeVar, MutableMapping, Optional, Iterable,
                    Callable, Any, Type, Union, overload)

from typing_extensions import Literal, Protocol

from ..domain import EPrint, EPrintDay, EPrintMonth, Listing, ListingMonth, \
    CanonicalRecord, VersionedIdentifier, Identifier, Version, CanonicalFile, \
    AllEPrints, ListingYear, AllListings, EPrintYear, Event, Canon
from ..integrity import IntegrityEntry, IntegrityVersion, IntegrityEPrint, \
    IntegrityListing, ManifestEntry, Manifest, IntegrityMonth, \
    Integrity, IntegrityDay, IntegrityListingMonth, \
    IntegrityListingYear, IntegrityListings, IntegrityYear, \
    IntegrityEPrints, IntegrityBase, calculate_checksum, IntegrityEntryMembers
from ..serialize.record import ListingSerializer, VersionSerializer

from ..record import RecordEntry, RecordVersion, RecordEPrint, RecordBase, \
    RecordListing, RecordMonth, RecordDay, RecordYear, RecordEntryMembers, \
    RecordListings, RecordListingMonth, RecordListingYear, RecordEPrints, \
    Record

from .util import LazyMap, LazyMapView

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class ConsistencyError(Exception):
    ...


class NoSuchResource(Exception):
    """."""


class IManifestStorage(Protocol):
    """
    Manifest storage protocol.

    This could conceivably be stored separately from the canonical record
    content, so it is defined separately.
    """

    def store_manifest(self, key: str, manifest: Manifest) -> None:  # pylint: disable=unused-argument; this is a stub.
        ...  # pylint: disable=pointless-statement; this is a stub.

    def load_manifest(self, key: str) -> Manifest:  # pylint: disable=unused-argument; this is a stub.
        ...  # pylint: disable=pointless-statement; this is a stub.


class ICanonicalStorage(IManifestStorage, Protocol):
    """Storage protocol for the canonical record."""

    def store_entry(self, ri: IntegrityEntry) -> None:  # pylint: disable=unused-argument; this is a stub.
        ...  # pylint: disable=pointless-statement; this is a stub.

    def load_entry(self, key: str) -> IntegrityEntry:  # pylint: disable=unused-argument; this is a stub.
        ...  # pylint: disable=pointless-statement; this is a stub.

    def list_subkeys(self, key: str) -> List[str]:  # pylint: disable=unused-argument; this is a stub.
        ...  # pylint: disable=pointless-statement; this is a stub.


_Name = TypeVar('_Name')
_Domain = TypeVar('_Domain')
_Record = TypeVar('_Record', bound=Union[RecordBase, RecordEntry])
_Integrity = TypeVar('_Integrity', bound=IntegrityBase)
_Member = TypeVar('_Member', bound=Optional['Base'])
_MemberName = TypeVar('_MemberName')
_Self = TypeVar('_Self', bound='Base')


class Base(Generic[_Name, _Domain, _Record, _Integrity, _MemberName, _Member]):
    domain: _Domain
    domain_type: Type[_Domain]
    record: _Record
    record_type: Type[_Record]
    integrity: _Integrity
    integrity_type: Type[_Integrity]
    member: _Member
    member_type: Type[_Member]

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
    def _get_member_name_for_version(cls, version: Version) -> _MemberName:
        """Get the name of a member that contains an eprint version."""
        raise NotImplementedError('Must be implemented by a child class, if'
                                  ' supported')

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> _MemberName:
        """Get the name of a member that contains an event."""
        raise NotImplementedError('Must be implemented by a child class, if'
                                  ' supported')

    @classmethod
    def _get_members(cls, storage: ICanonicalStorage, manifest: Manifest) \
            -> MutableMapping[_MemberName, _Member]:
        return LazyMap([entry['key'] for entry in manifest['entries']],
                       partial(cls.member_type.load, storage))

    @classmethod
    def load(cls: Type[_Self], storage: ICanonicalStorage, name: _Name,
             checksum: Optional[str] = None) -> _Self:
        manifest_key = cls.record_type.make_manifest_key(name)
        try:
            manifest = storage.load_manifest(manifest_key)
        except Exception:    # TODO: need a storage exception here.
            manifest = Manifest(entries=[])

        members = cls._get_members(storage, manifest)
        domain = cls.domain_type(name, LazyMapView(members, _get_domain))
        record = cls.record_type(name, LazyMapView(members, _get_record))
        integrity = cls.integrity_type(
            name,
            record=record,
            members=LazyMapView(members, _get_integrity),
            manifest=manifest,
            checksum=checksum
        )
        return cls(
            name,
            domain=domain,
            record=record,
            integrity=integrity,
            members=members
        )

    @property
    def members(self) -> Optional[MutableMapping[_MemberName, _Member]]:
        return self._members

    def add_versions(self, storage: ICanonicalStorage,
                     *versions: Version) -> None:
        self.save_members(
            storage,
            self._add_versions(storage, versions,
                               self._get_member_name_for_version)
        )
        assert self.integrity.manifest is not None
        self.integrity.update_checksum()

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        # Save all of the members that received events, and update our
        # manifest.
        self.save_members(
            storage,
            self._add_events(storage, events,
                             self._get_member_name_for_event)
        )
        assert self.integrity.manifest is not None
        self.integrity.update_checksum()

    def iter_members(self, storage: ICanonicalStorage) -> Iterable[_Member]:
        assert self.members is not None
        return (self.members[name] for name in self.members)

    def save(self, storage: ICanonicalStorage) -> str:
        """Store this register collection."""
        storage.store_manifest(self.record.make_manifest_key(self.name),
                               self.integrity.manifest)
        return self.integrity.checksum

    def save_members(self, storage: ICanonicalStorage,
                     members: Iterable[_Member]) -> None:
        """Save members that have changed, and update our manifest."""
        for member in members:
            checksum = member.save(storage)
            assert checksum is not None

            found = False
            for entry in self.integrity.manifest['entries']:
                # Update existing manifest entry.
                if entry['key'] == member.integrity.manifest_name:
                    entry['checksum'] = checksum
                    found = True
                    break
            if not found:   # New manifest entry.
                self.integrity.manifest['entries'].append({
                    "key": member.integrity.manifest_name,
                    "checksum": member.integrity.checksum
                })

    def _add_events(self, storage: ICanonicalStorage, events: Iterable[Event],
                    fkey: Callable[[Event], Any]) -> Iterable[_Member]:
        assert self.members is not None
        altered = set()
        for key, m_events in groupby(sorted(events, key=fkey), key=fkey):
            member = self.members[key]
            member.add_events(storage, *m_events)
            altered.add(member)
        return iter(altered)

    def _add_versions(self, storage: ICanonicalStorage,
                      versions: Iterable[Version],
                      fkey: Callable[[Version], Any]) -> Iterable[_Member]:
        assert self.members is not None
        altered = set()
        for key, m_versions in groupby(sorted(versions, key=fkey), key=fkey):
            member = self.members[key]
            member.add_versions(storage, *m_versions)
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


class RegisterFile(Base[str, CanonicalFile, RecordEntry, IntegrityEntry, None, None]):
    domain_type = CanonicalFile
    record_type = RecordEntry
    integrity_type = IntegrityEntry
    member_type = type(None)

    def save(self, storage: ICanonicalStorage) -> str:
        """
        Save this file.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        storage.store_entry(self.integrity)
        self.integrity.update_checksum()
        return self.integrity.checksum


class RegisterVersion(Base[VersionedIdentifier,
                           Version,
                           RecordVersion,
                           IntegrityVersion,
                           str,
                           RegisterFile]):
    domain_type = Version
    record_type = RecordVersion
    integrity_type = IntegrityVersion
    member_type = RegisterFile

    @classmethod
    def create(cls, storage: ICanonicalStorage, version: Version) \
            -> 'RegisterVersion':
        record = VersionSerializer.serialize(version)
        integrity = IntegrityVersion.from_record(record)
        members = {}
        for integrity_member in integrity.iter_members():
            member = RegisterFile(integrity_member.name,
                                  domain=integrity_member.record.domain,
                                  record=integrity_member.record,
                                  integrity=integrity_member)
            member.save(storage)
            members[member.name] = member
        return cls(record.name, domain=version, record=record,
                   integrity=integrity, members=members)


class RegisterEPrint(Base[Identifier,
                          EPrint,
                          RecordEPrint,
                          IntegrityEPrint,
                          VersionedIdentifier,
                          RegisterVersion]):
    domain_type = EPrint
    record_type = RecordEPrint
    integrity_type = IntegrityEPrint
    member_type = RegisterVersion

    @classmethod
    def _get_member_name_for_version(cls, version: Version) \
            -> VersionedIdentifier:
        return version.versioned_identifier

    def _add_versions(self, storage: ICanonicalStorage,
                      versions: Iterable[Version],
                      fkey: Callable[[Version], Any]) \
            -> Iterable[RegisterVersion]:
        assert self.members is not None
        altered = set()
        for version in versions:
            key = fkey(version)
            if key in self.members:
                raise ConsistencyError('Version already exists')
            member = self.member_type.create(storage, version)
            self.members[key] = member
            altered.add(member)
        return iter(altered)

    def add_versions(self, storage: ICanonicalStorage,
                     *versions: Version) -> None:
        self.save_members(
            storage,
            self._add_versions(storage, versions,
                               self._get_member_name_for_version)
        )
        assert self.integrity.manifest is not None
        self.integrity.update_checksum()


class RegisterDay(Base[datetime.date,
                       EPrintDay,
                       RecordDay,
                       IntegrityDay,
                       Identifier,
                       RegisterEPrint]):
    domain_type = EPrintDay
    record_type = RecordDay
    integrity_type = IntegrityDay
    member_type = RegisterEPrint

    @classmethod
    def _get_member_name_for_version(cls, version: Version) -> Identifier:
        return version.identifier


class RegisterMonth(Base[YearMonth,
                         EPrintMonth,
                         RecordMonth,
                         IntegrityMonth,
                         datetime.date,
                         RegisterDay]):
    domain_type = EPrintMonth
    record_type = RecordMonth
    integrity_type = IntegrityMonth
    member_type = RegisterDay

    @classmethod
    def _get_member_name_for_version(cls, version: Version) -> datetime.date:
        return version.announced_date_first


class RegisterYear(Base[Year,
                        EPrintYear,
                        RecordYear,
                        IntegrityYear,
                        YearMonth,
                        RegisterMonth]):
    domain_type = EPrintYear
    record_type = RecordYear
    integrity_type = IntegrityYear
    member_type = RegisterMonth

    @classmethod
    def _get_member_name_for_version(cls, version: Version) -> YearMonth:
        return (version.identifier.year,
                version.identifier.month)


class RegisterEPrints(Base[str,
                           AllEPrints,
                           RecordEPrints,
                           IntegrityEPrints,
                           Year,
                           RegisterYear]):
    domain_type = AllEPrints
    record_type = RecordEPrints
    integrity_type = IntegrityEPrints
    member_type = RegisterYear

    @classmethod
    def _get_member_name_for_version(cls, version: Version) -> Year:
        return version.identifier.year


class RegisterListing(Base[datetime.date,
                           Listing,
                           RecordListing,
                           IntegrityListing,
                           None,
                           None]):
    domain_type = Listing
    record_type = RecordListing
    integrity_type = IntegrityListing
    member_type = type(None)

    @classmethod
    def load(cls: Type[_Self], storage: ICanonicalStorage,
             date: datetime.date, checksum: Optional[str] = None) -> _Self:
        """
        Load a listing from storage.

        This method is overridden since it uses a different member mapping
        struct than higher-level collection types.
        """
        manifest_key = RecordListing.make_manifest_key(date)
        try:
            manifest = storage.load_manifest(manifest_key)

            # Listing already exists.
            listing = storage.load_entry(RecordListing.make_key(date))
            integrity = IntegrityListing(
                date,
                members=IntegrityEntryMembers(listing=listing),
                manifest=manifest,
                checksum=checksum
            )
            record = RecordListing(
                date,
                members=RecordEntryMembers(listing=listing.record)
            )
            domain = ListingSerializer.deserialize(record)
        except Exception:
            # New listing.
            manifest = Manifest(entries=[])
            domain = Listing(date=date, events=[])
            record = ListingSerializer.serialize(domain)
            integrity = IntegrityListing.from_record(record)
        return cls(date, domain=domain, record=record, integrity=integrity)

    @classmethod
    def create(cls, storage: ICanonicalStorage, listing: Listing) \
            -> 'RegisterListing':
        record = ListingSerializer.serialize(listing)
        integrity = IntegrityListing.from_record(record)
        storage.store_entry(integrity.listing)
        return cls(record.name, domain=listing, record=record, integrity=integrity)

    def save(self, storage: ICanonicalStorage) -> str:
        """
        Save this listing record.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        storage.store_entry(self.integrity.listing)
        self.integrity.update_checksum()
        return self.integrity.checksum

    def add_events(self, _: ICanonicalStorage, *events: Event) -> None:
        """
        Add events to the terminal listing record.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        N = len(events)
        for i, event in enumerate(events):
            self.domain.events.insert(N + i, event)
        self.record = ListingSerializer.serialize(self.domain)
        self.integrity = IntegrityListing.from_record(self.record)


class RegisterListingMonth(Base[YearMonth,
                                ListingMonth,
                                RecordListingMonth,
                                IntegrityListingMonth,
                                datetime.date,
                                RegisterListing]):

    domain_type = ListingMonth
    record_type = RecordListingMonth
    integrity_type = IntegrityListingMonth
    member_type = RegisterListing

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> datetime.date:
        return event.event_date.date()

    def add_listing(self, storage: ICanonicalStorage, listing: Listing) -> None:
        assert self.members is not None
        member = RegisterListing.create(storage, listing)
        self.members[member.domain.date] = member
        self.integrity.extend_manifest(member.integrity)


class RegisterListingYear(Base[Year,
                               ListingYear,
                               RecordListingYear,
                               IntegrityListingYear,
                               YearMonth,
                               RegisterListingMonth]):
    domain_type = ListingYear
    record_type = RecordListingYear
    integrity_type = IntegrityListingYear
    member_type = RegisterListingMonth

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> YearMonth:
        return (event.event_date.year, event.event_date.month)


class RegisterListings(Base[str,
                            AllListings,
                            RecordListings,
                            IntegrityListings,
                            Year,
                            RegisterListingYear]):
    domain_type = AllListings
    record_type = RecordListings
    integrity_type = IntegrityListings
    member_type = RegisterListingYear

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> Year:
        return event.event_date.year




listings_key = Literal['listings']
eprints_key = Literal['eprints']
TopLevelNames = Union[listings_key, eprints_key]
TopLevelMembers = Union[RegisterListings, RegisterEPrints]


class TopLevelMapping(collections.abc.MutableMapping):
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
                    Canon,
                    Record,
                    Integrity,
                    TopLevelNames,
                    TopLevelMembers]):
    domain_type = Canon
    record_type = Record
    integrity_type = Integrity
    member_type = TopLevelMembers  # type: ignore

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> TopLevelNames:
        return 'listings'

    @classmethod
    def _get_member_name_for_version(cls, version: Version) -> TopLevelNames:
        return 'eprints'

    @classmethod
    def _get_members(cls, storage: ICanonicalStorage, manifest: Manifest) \
            -> TopLevelMapping:
        return TopLevelMapping(RegisterListings.load(storage, 'listings'),
                               RegisterEPrints.load(storage, 'eprints'))