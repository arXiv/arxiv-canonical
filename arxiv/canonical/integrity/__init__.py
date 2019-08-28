"""
Integrity structs and collections for the canonical record.

This module provides a class hierarchy for integrity and consistency-related
concerns pertaining to the canonical record. The classes herein generate
and validate checksums, and generate manifests.

In order to efficiently verify the completeness and integrity of the record (or
a replica of the record), and to identify the source of inconsistencies,
consistency checks are performed at several levels of granularity (e.g. entry,
day, month, year, global). The completeness and integrity of all or a part of
the arXiv collection can be verified by comparing the checksum values at the
corresponding level of granularity.

The way in which checksum values are calculated for each level is described
below. This is inspired by the strategy for checksum validation of large
chunked uploads to Amazon S3. All checksum values are md5 hashes, stored and
transmitted as URL-safe base64-encoded strings.

+---------+-------------------------+------------------+----------------------+
| Level   | Contents                | Completeness     | Integrity            |
+=========+=========================+==================+======================+
| File    | Binary data.            | Presence/absence | Hash of binary file  |
|         |                         | of descriptor.   | content.             |
+---------+-------------------------+------------------+----------------------+
| Version | Collection of metadata, | Presence         | Hash of concatenated |
|         | source, and render      | of files.        | (sorted by name)     |
|         | files.                  |                  | file hashes.         |
+---------+-------------------------+------------------+----------------------+
| E-Print | One or more sequential  | Presence of      | Hash of concatenated |
|         | versions                | version records. | (sorted) version     |
|         |                         |                  | hashes.              |
+---------+-------------------------+------------------+----------------------+
| Day     | All e-prints the first  | Presence of      | Hash of concatenated |
|         | version of which was    | e-print records. | (sorted) e-print     |
|         | announced on this day.  |                  | hashes.              |
+---------+-------------------------+------------------+----------------------+
| Month   | All e-prints the first  | Presence of day  | Hash of concatenated |
|         | version of which was    | records.         | (sorted) day hashes. |
|         | announced in this       |                  |                      |
|         | month.                  |                  |                      |
+---------+-------------------------+------------------+----------------------+
| Year    | All e-prints the first  | Presence of      | Hash of concatenated |
|         | version of which was    | month records.   | (sorted) month       |
|         | announced in this       |                  | hashes.              |
|         | year.                   |                  |                      |
+---------+-------------------------+------------------+----------------------+
| All     | All e-prints.           | Presence of year | Hash of concatenated |
|         |                         | records.         | (sorted) year        |
|         |                         |                  | hashes.              |
+---------+-------------------------+------------------+----------------------+

The same hierarchy is used for listing files, where the terminal bitstream
is the binary serialized manifest.

A global integrity collection, :class:`.Integrity` draws together the
e-print and listing hierarchies into a final, composite level.
"""

from datetime import date
from operator import attrgetter, itemgetter
from typing import IO, NamedTuple, List, Dict, Sequence, Optional, Tuple, \
    Mapping, Generic, TypeVar, Union, Iterable

from mypy_extensions import TypedDict

from ..domain import VersionedIdentifier, Identifier
from ..record import RecordBase, RecordEntry, \
    RecordListing, RecordListingMonth, RecordListingYear, RecordVersion, \
    RecordVersion, RecordEPrint, RecordDay, RecordMonth, RecordYear, \
    RecordListings, RecordEPrints, Record
from ..util import GenericMonoDict
from .manifest import Manifest, ManifestEntry
from .checksum import calculate_checksum
from .exceptions import ValidationError, ChecksumError

Year = int
Month = int
YearAndMonth = Tuple[int, int]


# These TypeVars are used as placeholders in the generic IntegrityBase class,
# below. To learn more about TypeVars and Generics, see
# https://mypy.readthedocs.io/en/latest/generics.html
_Name = TypeVar('_Name')
_Record = TypeVar('_Record', bound=Union[RecordBase, RecordEntry])
_MemberName = TypeVar('_MemberName')
_Member = TypeVar('_Member', bound=Optional['IntegrityBase'])


class IntegrityEntryMembers(GenericMonoDict[str, 'IntegrityEntry']):
    """
    A dict that returns only :class: `.IntegrityEntry` instances.

    Consistent with ``Mapping[str, IntegrityEntry]``.
    """

    def __getitem__(self, key: str) -> 'IntegrityEntry':
        value = dict.__getitem__(self, key)
        assert isinstance(value, IntegrityEntry)
        return value


class IntegrityBase(Generic[_Name, _Record, _MemberName, _Member]):
    """
    Generic base class for all integrity collections.

    Provides a uniform protocol for integrity collections, while allowing
    the name, record, member name, and member types to vary from subclass
    to subclass.
    """
    def __init__(self, name: _Name,
                 record: Optional[_Record] = None,
                 members: Optional[Mapping[_MemberName, _Member]] = None,
                 manifest: Optional[Manifest] = None,
                 checksum: Optional[str] = None) -> None:
        self._manifest = manifest
        self._checksum = checksum
        self._members = members
        self._record = record
        self.name = name

    @classmethod
    def make_manifest(cls, members: Mapping[_MemberName, _Member]) -> Manifest:
        """Make a :class:`.Manifest` for this integrity collection."""
        return Manifest(entries=[
            cls.make_manifest_entry(members[name]) for name in members
        ])

    @classmethod
    def make_manifest_entry(cls, member: _Member) -> ManifestEntry:
        return ManifestEntry(key=member.manifest_name,
                             checksum=member.checksum)

    @property
    def checksum(self) -> str:
        """The checksum of this integrity collection."""
        assert self._checksum is not None
        return self._checksum

    @property
    def is_valid(self) -> bool:
        """Indicates whether or not this collection has a valid checksum."""
        return bool(self.checksum == calculate_checksum(self.manifest))

    @property
    def manifest(self) -> Manifest:
        """The :class:`.Manifest` of this integrity collection."""
        assert self._manifest is not None
        return self._manifest

    @property
    def manifest_name(self) -> str:
        """Get the name of this object for a parent manifest."""
        return str(self.name)

    @property
    def members(self) -> Mapping[_MemberName, _Member]:
        """The members of this collection."""
        assert self._members is not None
        return self._members

    @property
    def record(self) -> _Record:
        """The record associated with this collection."""
        assert self._record is not None
        return self._record

    def extend_manifest(self, member: _Member) -> None:
        self.manifest['entries'].append(self.make_manifest_entry(member))
        self.update_checksum(calculate_checksum(self.manifest))

    def iter_members(self) -> Iterable[_Member]:
        return [self.members[name] for name in self.members]

    def update_checksum(self, checksum: str) -> None:
        """Set the checksum for this record."""
        self._checksum = checksum


class IntegrityEntry(IntegrityBase[str, RecordEntry, None, None]):
    """Integrity entry for a single bitstream in the record."""

    @property
    def manifest_entry(self) -> ManifestEntry:
        """Generate a manifest entry for this :class:`.IntegrityEntry`."""
        return {'key': self.record.key,
                'checksum': self.checksum,
                'size_bytes': self.record.size_bytes,
                'mime_type': self.record.content_type.mime_type}

    @classmethod
    def from_record(cls, record: RecordEntry) -> 'IntegrityEntry':
        """Generate an :class:`.IntegrityEntry` from a :class:`.RecordEntry."""
        return cls(name=record.key, record=record,
                   checksum=calculate_checksum(record))

    # This is redefined since the entry has no manifest; the record entry is
    # used instead.
    @property
    def is_valid(self) -> bool:
        """Indicates whether or not this collection has a valid checksum."""
        return bool(self.checksum == calculate_checksum(self.record))


# This may seem a little odd, but want to leave the possibility of multiple
# listing files per day.
class IntegrityListing(IntegrityBase[date,
                                     RecordListing,
                                     str,
                                     IntegrityEntry]):
    """
    Integrity collection of listings for a single day.

    Currently supports only one listing file per day, but can be extended in
    the future by modifying :func:`IntegrityListing.from_record`.
    """

    @classmethod
    def from_record(cls, record: RecordListing) -> 'IntegrityListing':
        """
        Generate an :class:`.IntegrityListing` from a :class:`.RecordListing`.

        Currently supports only one listing file per day, but can be extended
        in the future by accepting multiple :class:`.RecordListing` elements
        here.
        """
        members = {
            record.listing.name: IntegrityEntry.from_record(record.listing)
        }
        manifest = cls.make_manifest(members)
        return cls(record.name, members=members, manifest=manifest,
                   checksum=calculate_checksum(manifest))

    @property
    def listing(self) -> IntegrityEntry:
        assert 'listing' in self.members
        return self.members['listing']

    @property
    def manifest_name(self) -> str:
        """The name to use for this record in a parent manifest."""
        return self.name.isoformat()


class IntegrityListingMonth(IntegrityBase[YearAndMonth,
                                          RecordListingMonth,
                                          date,
                                          IntegrityListing]):
    """Integrity collection of listings for a single month."""

    @property
    def manifest_name(self) -> str:
        """The name to use for this record in a parent manifest."""
        return f'{self.year}-{str(self.month).zfill(2)}'

    @property
    def month(self) -> Month:
        """The numeric month represented by this collection."""
        return self.name[1]

    @property
    def year(self) -> Year:
        """The numeric year represented by this collection."""
        return self.name[0]


class IntegrityListingYear(IntegrityBase[Year,
                                         RecordListingYear,
                                         YearAndMonth,
                                         IntegrityListingMonth]):
    """Integrity collection of listings for a single year."""

    @property
    def year(self) -> Year:
        """The numeric year represented by this collection."""
        return self.name


class IntegrityListings(IntegrityBase[str,
                                      RecordListings,
                                      Year,
                                      IntegrityListingYear]):
    """Integrity collection of all listings."""


class IntegrityVersion(IntegrityBase[VersionedIdentifier,
                                     RecordVersion,
                                     str,
                                     IntegrityEntry]):
    """Integrity collection for an e-print version."""

    @classmethod
    def from_record(cls, version: RecordVersion) -> 'IntegrityVersion':
        """Get an :class:`.IntegrityVersion` from a :class:`.RecordVersion`."""
        metadata = IntegrityEntry.from_record(version.metadata)
        render = IntegrityEntry.from_record(version.render)
        source = IntegrityEntry.from_record(version.source)
        members = IntegrityEntryMembers(
            metadata=metadata,
            render=render,
            source=source
        )
        manifest = cls.make_manifest(members)
        return cls(version.identifier,
                   members=members,
                   manifest=manifest,
                   checksum=calculate_checksum(manifest))

    @property
    def metadata(self) -> IntegrityEntry:
        return self.members['metadata']

    @property
    def render(self) -> IntegrityEntry:
        return self.members['render']

    @property
    def source(self) -> IntegrityEntry:
        return self.members['source']


class IntegrityEPrint(IntegrityBase[Identifier,
                                    RecordEPrint,
                                    VersionedIdentifier,
                                    IntegrityVersion]):
    """Integrity collection for an :class:`.EPrint`."""


class IntegrityDay(IntegrityBase[date,
                                 RecordDay,
                                 Identifier,
                                 IntegrityEPrint]):
    """
    Integrity collection for e-prints associated with a single day.

    Specifically, this includes all versions of e-prints the first version of
    which was announced on this day.
    """

    @property
    def day(self) -> date:
        """The numeric day represented by this collection."""
        return self.name


class IntegrityMonth(IntegrityBase[YearAndMonth,
                                   RecordMonth,
                                   date,
                                   IntegrityDay]):
    """
    Integrity collection for e-prints associated with a single month.

    Specifically, this includes all versions of e-prints the first version of
    which was announced in this month.
    """

    @property
    def month(self) -> Month:
        """The numeric month represented by this collection."""
        return self.name[1]

    @property
    def year(self) -> Year:
        """The numeric year represented by this collection."""
        return self.name[0]


class IntegrityYear(IntegrityBase[Year,
                                  RecordYear,
                                  YearAndMonth,
                                  IntegrityMonth]):
    """
    Integrity collection for e-prints associated with a single year.

    Specifically, this includes all versions of e-prints the first version of
    which was announced in this year.
    """

    @property
    def year(self) -> Year:
        """The numeric year represented by this collection."""
        return self.name


class IntegrityEPrints(IntegrityBase[str,
                                     RecordEPrints,
                                     Year,
                                     IntegrityYear]):
    """Integrity collection for all e-prints in the canonical record."""


class TopLevelMembers(
        GenericMonoDict[str, Union[IntegrityEPrints, IntegrityListings]]
    ):
    """
    A dict that returns only top level members.

    Consistent with
    ``Mapping[str, Union[IntegrityEPrints, IntegrityListings]]``.
    """

    def __getitem__(self, key: str) \
            -> Union[IntegrityEPrints, IntegrityListings]:
        value = dict.__getitem__(self, key)
        assert isinstance(value, (IntegrityEPrints, IntegrityListings))
        return value


class Integrity(IntegrityBase[None,
                              Record,
                              str,
                              Union[IntegrityEPrints, IntegrityListings]]):
    """Global integrity collection."""
