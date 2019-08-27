import collections
import datetime
from functools import partial
from itertools import groupby
from operator import attrgetter
from typing import (Dict, Tuple, Iterator, List, KeysView, NamedTuple,
                    Generic, TypeVar, MutableMapping, Optional, Iterable,
                    Callable, Any)

from typing_extensions import Protocol

from ..domain import EPrint, Day, Month, Listing, ListingMonth, \
    CanonicalRecord, VersionedIdentifier, Identifier, Version, File, \
    AllEPrints, ListingYear, AllListings, Year, Event
from ..integrity import IntegrityEntry, IntegrityVersion, IntegrityEPrint, \
    IntegrityListing, ManifestEntry, Manifest, IntegrityMonth, \
    IntegrityDay, IntegrityListingRange, IntegrityListingMonth, \
    IntegrityListingYear, IntegrityAllListings, IntegrityYear, \
    IntegrityAllEPrints, BaseIntegrity, get_checksum
from ..serialize.record import RecordEntry, RecordVersion, RecordEPrint, \
    RecordListing, RecordListingRange, RecordMonth, RecordDay, RecordYear, \
    ListingSerializer, RecordAllListings, RecordListingMonth, \
    RecordListingYear, RecordAllEPrints

from .util import LazyMap, LazyMapView, LazySequence, LazySequenceView


class NoSuchResource(Exception):
    """."""


class IManifestStorage(Protocol):
    """
    Manifest storage protocol.

    This could conceivably be stored separately from the canonical record
    content, so it is defined separately.
    """

    def store_manifest(self, key: str, manifest: Manifest) -> None:
        ...

    def load_manifest(self, key: str) -> Manifest:
        ...


class ICanonicalStorage(IManifestStorage, Protocol):
    """Storage protocol for the canonical record."""

    def store_entry(self, ri: IntegrityEntry) -> None:
        ...

    def load_entry(self, key: str) -> IntegrityEntry:
        ...

    def list_subkeys(self, key: str) -> List[str]:
        ...



class IManifestable(Protocol):
    @property
    def manifest(self) -> Manifest:
        ...


Domain = TypeVar('Domain')
Record = TypeVar('Record')
Integrity = TypeVar('Integrity', bound=BaseIntegrity)
Member = TypeVar('Member', bound='_Register')
Key = TypeVar('Key')


class _Register(Generic[Domain, Record, Integrity, Key, Member]):
    def __init__(self,
                 domain: Domain,
                 record: Record,
                 integrity: Integrity,
                 members: Optional[MutableMapping[Key, Member]] = None) \
            -> None:
        self._domain = domain
        self._record = record
        self._integrity = integrity
        self._members = members

    @property
    def domain(self) -> Domain:
        return self._domain

    @property
    def record(self) -> Record:
        return self._record

    @property
    def integrity(self) -> Integrity:
        return self._integrity

    @property
    def members(self) -> Optional[MutableMapping[Key, Member]]:
        return self._members

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        ...

    def save(self, storage: ICanonicalStorage) -> str:
        ...

    def save_members(self, storage: ICanonicalStorage,
                     members: Iterable[Member]) -> None:
        """Save members that have changed, and update our manifest."""
        for member in members:
            checksum = member.save(storage)
            assert checksum is not None

            found = False
            for entry in self.integrity.manifest['entries']:
                # Update existing manifest entry.
                if entry['key'] == member.integrity.manifest_key:
                    entry['checksum'] = checksum
                    found = True
                    break
            if not found:   # New manifest entry.
                self.integrity.manifest['entries'].append({
                    "key": member.integrity.manifest_key,
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


def get_domain(register: _Register[Domain,
                                   Record,
                                   Integrity,
                                   Key,
                                   Member]) -> Domain:
    return register.domain


def get_record(register: _Register[Domain,
                                   Record,
                                   Integrity,
                                   Key,
                                   Member]) -> Record:
    return register.record


def get_integrity(register: _Register[Domain,
                                      Record,
                                      Integrity,
                                      Key,
                                      Member]) -> Integrity:
    return register.integrity


class RegisterFile(_Register[File, RecordEntry, IntegrityEntry, None, None]):
    pass


class RegisterVersion(_Register[Version,
                                RecordVersion,
                                IntegrityVersion,
                                str,
                                RegisterFile]):
    pass


class RegisterEPrint(_Register[EPrint,
                               RecordEPrint,
                               IntegrityEPrint,
                               VersionedIdentifier,
                               RegisterVersion]):
    pass


class RegisterDay(_Register[Day,
                            RecordDay,
                            IntegrityDay,
                            Identifier,
                            RegisterEPrint]):
    pass


class RegisterMonth(_Register[Month,
                              RecordMonth,
                              IntegrityMonth,
                              datetime.date,
                              RegisterDay]):
    pass


class RegisterYear(_Register[Year,
                             RecordYear,
                             IntegrityYear,
                             Tuple[int, int],
                             RegisterMonth]):
    pass


class RegisterAllEPrints(_Register[AllEPrints,
                                   RecordAllEPrints,
                                   IntegrityAllEPrints,
                                   int,
                                   RegisterYear]):
    pass


class RegisterListing(_Register[Listing,
                                RecordListing,
                                IntegrityListing,
                                None,
                                None]):
    @classmethod
    def load(cls, storage: ICanonicalStorage, date: datetime.date,
             checksum: Optional[str] = None) -> 'RegisterListing':
        manifest_key = RecordListing.make_manifest_key(date)  # pylint: disable=no-member
        try:
            manifest = storage.load_manifest(manifest_key)

            # Listing already exists.
            integrity = IntegrityListing(
                date=date,
                listing=storage.load_entry(RecordListing.make_key(date)),  # pylint: disable=no-member
                manifest=manifest,
                checksum=checksum
            )
            record = RecordListing(date=date, listing=integrity.listing.record)
            domain = ListingSerializer.deserialize(record)
        except Exception:
            # New listing.
            manifest = Manifest(entries=[])
            domain = Listing(date=date, events=[])
            record = ListingSerializer.serialize(domain)
            integrity = IntegrityListing.from_record(record)
        return cls(domain=domain, record=record, integrity=integrity)

    @classmethod
    def create(cls, storage: ICanonicalStorage, listing: Listing) \
            -> 'RegisterListing':
        record = ListingSerializer.serialize(listing)
        integrity = IntegrityListing.from_record(record)  # pylint: disable=no-member
        storage.store_entry(integrity.listing)
        return cls(domain=listing, record=record, integrity=integrity)

    def save(self, storage: ICanonicalStorage) -> str:
        storage.store_entry(self.integrity.listing)
        self.update_checksum(get_checksum(self.integrity.manifest))
        return self.integrity.checksum

    def add_event(self, storage: ICanonicalStorage, event: Event) -> None:
        self.domain.events.insert(len(self.domain.events), event)
        self._record = ListingSerializer.serialize(self.domain)
        self._integrity = IntegrityListing.from_record(self._record)  # pylint: disable=no-member

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        N = len(events)
        for i, event in enumerate(events):
            self.domain.events.insert(N + i, event)
        self._record = ListingSerializer.serialize(self.domain)
        self._integrity = IntegrityListing.from_record(self._record)  # pylint: disable=no-member


class RegisterListingMonth(_Register[ListingMonth,
                                     RecordListingMonth,
                                     IntegrityListingMonth,
                                     datetime.date,
                                     RegisterListing]):

    @classmethod
    def load(cls, storage: ICanonicalStorage, key: Tuple[int, int],
             checksum: Optional[str] = None) -> 'RegisterListingMonth':
        year, month = key
        manifest_key = RecordListingMonth.make_manifest_key(year, month)  # pylint: disable=no-member
        try:
            manifest = storage.load_manifest(manifest_key)
            listings = LazyMap([entry['key'] for entry in manifest['entries']],
                                partial(RegisterListing.load, storage))
        except Exception:    # TODO: need a storage exception here.
            manifest = Manifest(entries=[])
            listings = LazyMap([], partial(RegisterListing.load, storage))

        return cls(
            domain=ListingMonth(
                year=year,
                month=month,
                listings=LazyMapView(listings, get_domain)
            ),
            record=RecordListingMonth(
                year=year,
                month=month,
                days=LazyMapView(listings, get_record)
            ),
            integrity=IntegrityListingMonth(
                year=year,
                month=month,
                days=LazyMapView(listings, get_integrity),
                manifest=manifest,
                checksum=checksum
            ),
            members=listings
        )

    def add_event(self, storage: ICanonicalStorage, event: Event) -> None:
        assert self.members is not None
        member = self.members[event.event_date]
        member.add_event(storage, event)
        member.save(storage)

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        key_fnc = lambda event: event.event_date.date()
        # Save all of the members that received events, and update our
        # manifest.
        self.save_members(storage, self._add_events(storage, events, key_fnc))
        assert self.integrity.manifest is not None
        self.update_checksum(get_checksum(self.integrity.manifest))

    def add_listing(self, storage: ICanonicalStorage, listing: Listing) -> None:
        assert self.integrity.manifest is not None
        assert self.members is not None
        member = RegisterListing.create(storage, listing)
        self.members[member.domain.date] = member
        manifest_entry = IntegrityListingMonth.make_manifest_entry(member.integrity)  # pylint: disable=no-member

        self.integrity.manifest['entries'].append(manifest_entry)
        self.update_checksum(get_checksum(self.integrity.manifest))

    def save(self, storage: ICanonicalStorage) -> str:
        assert self.integrity.manifest is not None
        storage.store_manifest(
            RecordListingMonth.make_manifest_key(self.domain.year,    # pylint: disable=no-member
                                                 self.domain.month),
            self.integrity.manifest
        )
        assert self.integrity.checksum is not None
        return self.integrity.checksum


class RegisterListingYear(_Register[ListingYear,
                                    RecordListingYear,
                                    IntegrityListingYear,
                                    Tuple[int, int],
                                    RegisterListingMonth]):
    @classmethod
    def load(cls, storage: ICanonicalStorage, year: int,
             checksum: Optional[str] = None) -> 'RegisterListingYear':
        manifest_key = RecordListingYear.make_manifest_key(year)  # pylint: disable=no-member
        try:
            manifest = storage.load_manifest(manifest_key)
            months = LazyMap([entry['key'] for entry in manifest['entries']],
                             partial(RegisterListingMonth.load, storage))
        except Exception:    # TODO: need a storage exception here.
            manifest = Manifest(entries=[])
            months = LazyMap([], partial(RegisterListingMonth.load, storage))

        return cls(
            domain=ListingYear(
                year=year,
                months=LazyMapView(months, get_domain)
            ),
            record=RecordListingYear(
                year=year,
                months=LazyMapView(months, get_record)
            ),
            integrity=IntegrityListingYear(
                year=year,
                months=LazyMapView(months, get_integrity),
                manifest=manifest,
                checksum=checksum
            ),
            members=months
        )

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        key_fnc = lambda event: (event.event_date.year, event.event_date.month)
        # Save all of the members that received events, and update our
        # manifest.
        self.save_members(storage, self._add_events(storage, events, key_fnc))
        assert self.integrity.manifest is not None
        self.update_checksum(get_checksum(self.integrity.manifest))

    def save(self, storage: ICanonicalStorage) -> str:
        assert self.integrity.manifest is not None
        storage.store_manifest(
            RecordListingYear.make_manifest_key(self.domain.year),    # pylint: disable=no-member
            self.integrity.manifest
        )
        assert self.integrity.checksum is not None
        return self.integrity.checksum


class RegisterAllListings(_Register[AllListings,
                                    RecordAllListings,
                                    IntegrityAllListings,
                                    int,
                                    RegisterListingYear]):
    @classmethod
    def load(cls, storage: ICanonicalStorage,
             checksum: Optional[str] = None) -> 'RegisterAllListings':
        manifest_key = RecordAllListings.make_manifest_key()  # pylint: disable=no-member
        try:
            manifest = storage.load_manifest(manifest_key)
            years = LazyMap([entry['key'] for entry in manifest['entries']],
                            partial(RegisterListingYear.load, storage))
        except Exception:    # TODO: need a storage exception here.
            manifest = Manifest(entries=[])
            years = LazyMap([], partial(RegisterListingYear.load, storage))
            print('excepted')

        print('The manifest!', manifest)

        return cls(
            domain=AllListings(
                years=LazyMapView(years, get_domain)
            ),
            record=RecordAllListings(
                years=LazyMapView(years, get_record)
            ),
            integrity=IntegrityAllListings(
                years=LazyMapView(years, get_integrity),
                manifest=manifest,
                checksum=checksum
            ),
            members=years
        )

    def add_events(self, storage: ICanonicalStorage, *events: Event) -> None:
        # Save all of the members that received events, and update our
        # manifest.
        key_fnc = attrgetter('event_date.year')
        self.save_members(storage, self._add_events(storage, events, key_fnc))
        assert self.integrity.manifest is not None
        self.update_checksum(get_checksum(self.integrity.manifest))

    def save(self, storage: ICanonicalStorage) -> str:
        storage.store_manifest(
            RecordAllListings.make_manifest_key(),    # pylint: disable=no-member
            self.integrity.manifest
        )
        return self.integrity.checksum
