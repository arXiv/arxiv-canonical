# import collections
# import datetime
# from functools import partial
# from operator import attrgetter
# from typing import Tuple, List, Optional, TypeVar, Callable, Dict, \
#     MutableMapping, Iterable, cast, Any, Type, Iterator, Mapping, Sequence, \
#     Generic

# from backports.datetime_fromisoformat import MonkeyPatch

# from .base import ICanonicalStorage, RegisterVersion, RegisterListing, \
#     RegisterEPrint, RegisterMonth, RegisterDay, \
#     RegisterListingMonth, get_domain, get_integrity, get_record, \
#     Domain, Record, Integrity, Manifest

# from .util import LazyMap, LazyMapView, LazyMap, LazyMapView

# from ..domain import Identifier, VersionedIdentifier, Version, Listing, \
#     EPrint, EPrintMonth, EPrintDay, ListingMonth
# from ..serialize.record import VersionSerializer, ListingSerializer, \
#     MetadataSerializer
# from ..record import RecordVersion, RecordListing, \
#     RecordEPrint, RecordDay, RecordMonth
# from ..integrity import IntegrityVersion, IntegrityListing, IntegrityEPrint, \
#     ValidationError, IntegrityDay, IntegrityMonth, IntegrityListingMonth

# MonkeyPatch.patch_fromisoformat()


# def store_version(storage: ICanonicalStorage, version: Version) -> None:
#     ie = IntegrityVersion.from_record(VersionSerializer.serialize(version))
#     map(storage.store_entry, ie.iter_entries())



# # def mapping_type(key_type: KeyType, value_type: ValueType) \
# #         -> Type[MutableMapping[KeyType, ValueType]]:



# # def load_record(storage: ICanonicalStorage) -> CanonicalRecord:
# #     """
# #     Initialize and return the :class:`.CanonicalRecord`.
# #     """
# #     raise NotImplementedError('Implement me!')


# # def load_block(storage: ICanonicalStorage, year: int, month: int) -> Month:
# #     """
# #     Load a :class:`.Month`.

# #     Parameters
# #     ----------
# #     year : int
# #     month : int

# #     Returns
# #     -------
# #     :class:`.Month`

# #     """



# # def store_listing(storage: ICanonicalStorage, listing: Listing) -> None:
# #     """
# #     Store a :class:`.Listing`.

# #     Should complain loudly if ``self.read_only`` is ``True``.
# #     """
# #     if self.read_only:
# #         raise RuntimeError('This is a read-only session')
# #     record = listing_serializer.serialize(listing)
# #     self._write_key(record.key, record.content, record.checksum)

# # def load_listing(storage: ICanonicalStorage, listing_date: date) -> Listing:
# #     """
# #     Load a :class:`.Listing`.

# #     If ``self.read_only`` is ``False``, the ``events`` member of the
# #     listing must be a subclass of ``list``, and implement an
# #     ``append(event: Event) -> None`` method that, when called, writes the
# #     current state of the listing to S3.

# #     Parameters
# #     ----------
# #     listing_date : datetime
# #         Date for selecting listing events.

# #     Returns
# #     -------
# #     :class:`.Listing`

# #     """
# #     record = listing_serializer.load(listing_date, self._loader)
# #     return listing_serializer.deserialize(record, self)

# # def store_eprint(storage: ICanonicalStorage, e_print: EPrint) -> None:
# #     """
# #     Store an :class:`.EPrint`.

# #     If the :attr:`.EPrint.source` or :attr:`.EPrint.pdf` content
# #     has changed, those should also be stored.

# #     Should complain loudly if ``self.read_only`` is ``True``.
# #     """
# #     if self.read_only:
# #         raise RuntimeError('This is a read-only session')
# #     for key, entry in eprint_serializer.serialize(e_print).iter_members():
# #         self._write_key(key, entry.content, entry.checksum)

# # def load_eprint(storage: ICanonicalStorage, identifier: VersionedIdentifier) -> EPrint:
# #     """
# #     Load an :class:`.EPrint`.

# #     The content of the :attr:`.EPrint.source` and
# #     :attr:`.EPrint.pdf.content` should provide a ``read()`` method that,
# #     when called, retrieves the content of the corresponding resource from
# #     storage.
# #     """
# #     record = eprint_serializer.load(identifier.arxiv_id,
# #                                     identifier.version, self._loader)
# #     return eprint_serializer.deserialize(record)
