import collections
import datetime
from functools import partial
from itertools import groupby
from operator import attrgetter
from typing import (Dict, Tuple, Iterator, List, KeysView, NamedTuple,
                    Generic, TypeVar, MutableMapping, Optional, Iterable,
                    Callable, Any, Type, Union)

from typing_extensions import Protocol

from ..domain import EPrint, EPrintDay, EPrintMonth, Listing, ListingMonth, \
    CanonicalRecord, VersionedIdentifier, Identifier, Version, File, \
    AllEPrints, ListingYear, AllListings, EPrintYear, Event
from ..integrity import IntegrityEntry, IntegrityVersion, IntegrityEPrint, \
    IntegrityListing, ManifestEntry, Manifest, IntegrityMonth, \
    IntegrityDay, IntegrityListingMonth, \
    IntegrityListingYear, IntegrityListings, IntegrityYear, \
    IntegrityEPrints, IntegrityBase, calculate_checksum, IntegrityEntryMembers
from ..serialize.record import ListingSerializer

from ..record import RecordEntry, RecordVersion, RecordEPrint, RecordBase, \
    RecordListing, RecordMonth, RecordDay, RecordYear, RecordEntryMembers, \
    RecordAllListings, RecordListingMonth, RecordListingYear, RecordAllEPrints

from .util import LazyMap, LazyMapView, LazyMap, LazyMapView

Year = int
Month = int
YearMonth = Tuple[Year, Month]


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


Name = TypeVar('Name')
Domain = TypeVar('Domain')
Record = TypeVar('Record', bound=Union[RecordBase, RecordEntry])
Integrity = TypeVar('Integrity', bound=IntegrityBase)
Member = TypeVar('Member', bound=Optional['_Register'])
MemberName = TypeVar('MemberName')
Self = TypeVar('Self', bound='_Register')


class _Register(Generic[Name, Domain, Record, Integrity, MemberName, Member]):
    domain: Domain
    domain_type: Type[Domain]
    record: Record
    record_type: Type[Record]
    integrity: Integrity
    integrity_type: Type[Integrity]
    member: Member
    member_type: Type[Member]

    def __init__(self,
                 name: Name,
                 domain: Domain,
                 record: Record,
                 integrity: Integrity,
                 members: Optional[MutableMapping[MemberName, Member]] = None) \
            -> None:
        self.domain = domain
        self.record = record
        self.integrity = integrity
        self.name = name
        self._members = members

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> MemberName:
        """Get the name of a member that contains an event."""
        raise NotImplementedError('Must be implemented by a child class, if'
                                  ' supported')

    @classmethod
    def load(cls: Type[Self], storage: ICanonicalStorage, name: Name,
             checksum: Optional[str] = None) -> Self:
        manifest_key = cls.record_type.make_manifest_key(name)
        try:
            manifest = storage.load_manifest(manifest_key)
            members = LazyMap([entry['key'] for entry in manifest['entries']],
                             partial(cls.member_type.load, storage))
        except Exception:    # TODO: need a storage exception here.
            manifest = Manifest(entries=[])
            members = LazyMap([], partial(cls.member_type.load, storage))
        domain = cls.domain_type(name, LazyMapView(members, get_domain))
        record = cls.record_type(name, LazyMapView(members, get_record))
        integrity = cls.integrity_type(
            name,
            record=record,
            members=LazyMapView(members, get_integrity),
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
    def members(self) -> Optional[MutableMapping[MemberName, Member]]:
        return self._members

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        # Save all of the members that received events, and update our
        # manifest.
        self.save_members(storage, self._add_events(storage, events,
                          self._get_member_name_for_event))
        assert self.integrity.manifest is not None
        self.update_checksum(calculate_checksum(self.integrity.manifest))

    def save(self, storage: ICanonicalStorage) -> str:
        """Store this register collection."""
        storage.store_manifest(self.record.make_manifest_key(self.name),
                               self.integrity.manifest)
        return self.integrity.checksum

    def save_members(self, storage: ICanonicalStorage,
                     members: Iterable[Member]) -> None:
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

    def update_checksum(self, checksum: str) -> None:
        self.integrity.update_checksum(checksum)

    def _add_events(self, storage: ICanonicalStorage, events: Iterable[Event],
                    key_fnc: Callable[[Event], Any]) -> Iterable[Member]:
        assert self.members is not None
        altered = set()
        for key, m_events in groupby(sorted(events, key=key_fnc), key=key_fnc):
            member = self.members[key]
            member.add_events(storage, *m_events)
            altered.add(member)
        return iter(altered)


def get_domain(register: _Register[Name,
                                   Domain,
                                   Record,
                                   Integrity,
                                   MemberName,
                                   Member]) -> Domain:
    return register.domain


def get_record(register: _Register[Name,
                                   Domain,
                                   Record,
                                   Integrity,
                                   MemberName,
                                   Member]) -> Record:
    return register.record


def get_integrity(register: _Register[Name,
                                      Domain,
                                      Record,
                                      Integrity,
                                      MemberName,
                                      Member]) -> Integrity:
    return register.integrity


class RegisterFile(_Register[str,
                             File,
                             RecordEntry,
                             IntegrityEntry,
                             None,
                             None]):
    domain_type = File
    record_type = RecordEntry
    integrity_type = IntegrityEntry
    member_type = type(None)


class RegisterVersion(_Register[VersionedIdentifier,
                                Version,
                                RecordVersion,
                                IntegrityVersion,
                                str,
                                RegisterFile]):
    domain_type = Version
    record_type = RecordVersion
    integrity_type = IntegrityVersion
    member_type = RegisterFile


class RegisterEPrint(_Register[Identifier,
                               EPrint,
                               RecordEPrint,
                               IntegrityEPrint,
                               VersionedIdentifier,
                               RegisterVersion]):
    domain_type = EPrint
    record_type = RecordEPrint
    integrity_type = IntegrityEPrint
    member_type = RegisterVersion


class RegisterDay(_Register[datetime.date,
                            EPrintDay,
                            RecordDay,
                            IntegrityDay,
                            Identifier,
                            RegisterEPrint]):
    domain_type = EPrintDay
    record_type = RecordDay
    integrity_type = IntegrityDay
    member_type = RegisterEPrint


class RegisterMonth(_Register[YearMonth,
                              EPrintMonth,
                              RecordMonth,
                              IntegrityMonth,
                              datetime.date,
                              RegisterDay]):
    domain_type = EPrintMonth
    record_type = RecordMonth
    integrity_type = IntegrityMonth
    member_type = RegisterDay


class RegisterYear(_Register[Year,
                             EPrintYear,
                             RecordYear,
                             IntegrityYear,
                             YearMonth,
                             RegisterMonth]):
    domain_type = EPrintYear
    record_type = RecordYear
    integrity_type = IntegrityYear
    member_type = RegisterMonth


class RegisterAllEPrints(_Register[str,
                                   AllEPrints,
                                   RecordAllEPrints,
                                   IntegrityEPrints,
                                   int,
                                   RegisterYear]):
    domain_type = AllEPrints
    record_type = RecordAllEPrints
    integrity_type = IntegrityEPrints
    member_type = RegisterYear


class RegisterListing(_Register[datetime.date,
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
    def load(cls: Type[Self], storage: ICanonicalStorage,
             date: datetime.date, checksum: Optional[str] = None) -> Self:
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
        self.update_checksum(calculate_checksum(self.integrity.manifest))
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


class RegisterListingMonth(_Register[YearMonth,
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


class RegisterListingYear(_Register[Year,
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


class RegisterAllListings(_Register[str,
                                    AllListings,
                                    RecordAllListings,
                                    IntegrityListings,
                                    int,
                                    RegisterListingYear]):
    domain_type = AllListings
    record_type = RecordAllListings
    integrity_type = IntegrityListings
    member_type = RegisterListingYear

    @classmethod
    def _get_member_name_for_event(cls, event: Event) -> Year:
        return event.event_date.year

