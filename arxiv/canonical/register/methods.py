import collections
import datetime
from functools import partial
from operator import attrgetter
from typing import Tuple, List, Optional, TypeVar, Callable, Dict, \
    MutableMapping, Iterable, cast, Any, Type, Iterator, Mapping, Sequence, \
    Generic

from backports.datetime_fromisoformat import MonkeyPatch

from .base import ICanonicalStorage, RegisterVersion, RegisterListing, \
    RegisterEPrint, RegisterMonth, RegisterDay, \
    RegisterListingMonth, get_domain, get_integrity, get_record, \
    Domain, Record, Integrity, Manifest

from .util import LazyMap, LazyMapView, LazyMap, LazyMapView

from ..domain import Identifier, VersionedIdentifier, Version, Listing, \
    EPrint, EPrintMonth, EPrintDay, ListingMonth
from ..serialize.record import VersionSerializer, ListingSerializer, \
    MetadataSerializer
from ..record import RecordVersion, RecordListing, \
    RecordEPrint, RecordDay, RecordMonth
from ..integrity import IntegrityVersion, IntegrityListing, IntegrityEPrint, \
    ValidationError, IntegrityDay, IntegrityMonth, IntegrityListingMonth

MonkeyPatch.patch_fromisoformat()


def store_version(storage: ICanonicalStorage, version: Version) -> None:
    ie = IntegrityVersion.from_record(VersionSerializer.serialize(version))
    map(storage.store_entry, ie.iter_entries())


def load_version(storage: ICanonicalStorage, identifier: VersionedIdentifier,
                 checksum: Optional[str] = None) -> RegisterVersion:
    integrity_meta = storage.load_entry(VersionSerializer.make_key(identifier))
    metadata = MetadataSerializer.deserialize(integrity_meta.record)
    assert metadata.render is not None and metadata.source is not None
    integrity_render = storage.load_entry(
        VersionSerializer.make_key(identifier, metadata.render.filename)
    )
    integrity_source = storage.load_entry(
        VersionSerializer.make_key(identifier, metadata.source.filename)
    )
    record = RecordVersion(
        identifier=identifier,
        metadata=integrity_meta.record,
        render=integrity_render.record,
        source=integrity_source.record
    )
    return RegisterVersion(
        domain=VersionSerializer.deserialize(record),
        record=record,
        integrity=IntegrityVersion(
            identifier=identifier,
            metadata=integrity_meta,
            render=integrity_render,
            source=integrity_source,
            checksum=checksum
        )
    )


def load_eprint(storage: ICanonicalStorage, identifier: Identifier,
                checksum: Optional[str] = None) -> RegisterEPrint:
    keys = [VersionedIdentifier(f'{identifier}{key}') for key
            in storage.list_subkeys(EPrintSerializer.make_key(identifier))]
    versions = LazyMap(keys, partial(load_version, storage))

    return RegisterEPrint(
        domain=EPrint(
            identifier=identifier,
            versions=LazyMapView(versions, get_domain)
        ),
        record=RecordEPrint(
            identifier=identifier,
            versions=LazyMapView(versions, get_record)
        ),
        integrity=IntegrityEPrint(
            identifier=identifier,
            versions=LazyMapView(versions, get_integrity),
            checksum=checksum
        )
    )


def load_daily_block(storage: ICanonicalStorage, date: datetime.date,
                     checksum: Optional[None] = None) -> RegisterDay:
    manifest = storage.load_manifest(DaySerializer.make_manifest_key(date))
    keys = [Identifier(entry['key']) for entry in manifest.entries]
    mapping: MutableMapping[datetime.date, RegisterDay]
    mapping = LazyMap(keys, partial(load_eprint, storage))
    return RegisterDay(
        domain=EPrintDay(
            date=date,
            eprints=LazyMapView(mapping, get_domain)
        ),
        record=RecordDay(
            date=date,
            eprints=LazyMapView(mapping, get_record)
        ),
        integrity=IntegrityDay(
            date=date,
            eprints=LazyMapView(mapping, get_integrity),
            checksum=checksum
        )
    )


def load_monthly_block(storage: ICanonicalStorage, year: int, month: int,
                       checksum: Optional[str] = None) -> RegisterMonth:
    key = MonthSerializer.make_manifest_key(year, month)
    manifest = storage.load_manifest(key)

    keys: List[datetime.date] = [datetime.date.fromisoformat(et['key']) for et in manifest.entries]
    mapping: MutableMapping[datetime.date, RegisterDay]
    mapping = LazyMap(keys, partial(load_daily_block, storage))
    return RegisterMonth(
        domain=EPrintMonth(
            year=year,
            month=month,
            days=LazyMapView(mapping, get_domain)
        ),
        record=RecordMonth(
            year=year,
            month=month,
            days=LazyMapView(mapping, get_record)
        ),
        integrity=IntegrityMonth(
            year=year,
            month=month,
            days=LazyMapView(mapping, get_integrity),
            checksum=checksum
        )
    )



def load_listing_range(storage: ICanonicalStorage, start_date: datetime.date,
                       end_date: datetime.date,
                       checksum: Optional[str] = None) -> RegisterListingMonth:

    keys = [start_date + datetime.timedelta(days=i)
            for i in range((end_date - start_date).days + 1)]
    listings = LazyMap(keys, partial(load_listing, storage))
    return RegisterListingRange(
        domain=ListingRange(
            start_date=start_date,
            end_date=end_date,
            listings=LazyMapView(listings, get_domain)
        ),
        record=RecordListingRange(
            start_date=start_date,
            end_date=end_date,
            listings=LazyMapView(listings, get_record)
        ),
        integrity=IntegrityListingRange(
            start_date=start_date,
            end_date=end_date,
            listings=LazyMapView(listings, get_integrity),
            manifest=None,
            checksum=checksum
        )
    )





# def mapping_type(key_type: KeyType, value_type: ValueType) \
#         -> Type[MutableMapping[KeyType, ValueType]]:



# def load_record(storage: ICanonicalStorage) -> CanonicalRecord:
#     """
#     Initialize and return the :class:`.CanonicalRecord`.
#     """
#     raise NotImplementedError('Implement me!')


# def load_block(storage: ICanonicalStorage, year: int, month: int) -> Month:
#     """
#     Load a :class:`.Month`.

#     Parameters
#     ----------
#     year : int
#     month : int

#     Returns
#     -------
#     :class:`.Month`

#     """



# def store_listing(storage: ICanonicalStorage, listing: Listing) -> None:
#     """
#     Store a :class:`.Listing`.

#     Should complain loudly if ``self.read_only`` is ``True``.
#     """
#     if self.read_only:
#         raise RuntimeError('This is a read-only session')
#     record = listing_serializer.serialize(listing)
#     self._write_key(record.key, record.content, record.checksum)

# def load_listing(storage: ICanonicalStorage, listing_date: date) -> Listing:
#     """
#     Load a :class:`.Listing`.

#     If ``self.read_only`` is ``False``, the ``events`` member of the
#     listing must be a subclass of ``list``, and implement an
#     ``append(event: Event) -> None`` method that, when called, writes the
#     current state of the listing to S3.

#     Parameters
#     ----------
#     listing_date : datetime
#         Date for selecting listing events.

#     Returns
#     -------
#     :class:`.Listing`

#     """
#     record = listing_serializer.load(listing_date, self._loader)
#     return listing_serializer.deserialize(record, self)

# def store_eprint(storage: ICanonicalStorage, e_print: EPrint) -> None:
#     """
#     Store an :class:`.EPrint`.

#     If the :attr:`.EPrint.source` or :attr:`.EPrint.pdf` content
#     has changed, those should also be stored.

#     Should complain loudly if ``self.read_only`` is ``True``.
#     """
#     if self.read_only:
#         raise RuntimeError('This is a read-only session')
#     for key, entry in eprint_serializer.serialize(e_print).iter_members():
#         self._write_key(key, entry.content, entry.checksum)

# def load_eprint(storage: ICanonicalStorage, identifier: VersionedIdentifier) -> EPrint:
#     """
#     Load an :class:`.EPrint`.

#     The content of the :attr:`.EPrint.source` and
#     :attr:`.EPrint.pdf.content` should provide a ``read()`` method that,
#     when called, retrieves the content of the corresponding resource from
#     storage.
#     """
#     record = eprint_serializer.load(identifier.arxiv_id,
#                                     identifier.version, self._loader)
#     return eprint_serializer.deserialize(record)
