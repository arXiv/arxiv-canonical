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
    Mapping, Generic, TypeVar, Union, Iterable, Type

from mypy_extensions import TypedDict
from typing_extensions import Literal

from ..domain import VersionedIdentifier, Identifier, ListingIdentifier, \
    Listing, Version, CanonicalBaseCollection, EventType
# from ..record import RecordBase, RecordStream, RecordListingDay, \
#     RecordListing, RecordListingMonth, RecordListingYear, RecordVersion, \
#     RecordVersion, RecordEPrint, RecordDay, RecordMonth, RecordYear, \
#     RecordListings, RecordEPrints, Record, RecordMetadata, RecordEntry
from .. import record as R
from ..util import GenericMonoDict
from .manifest import Manifest, ManifestEntry, ManifestDecoder, \
    ManifestEncoder, make_empty_manifest
from .checksum import calculate_checksum
from .exceptions import ValidationError, ChecksumError

Year = int
Month = int
YearAndMonth = Tuple[int, int]


# These TypeVars are used as placeholders in the generic IntegrityBase class,
# below. To learn more about TypeVars and Generics, see
# https://mypy.readthedocs.io/en/latest/generics.html
_Name = TypeVar('_Name')
_Record = TypeVar('_Record', bound=Union[R.RecordBase, R.RecordEntry])
_MemberName = TypeVar('_MemberName')
_Member = TypeVar('_Member', bound=Optional['IntegrityBase'])
_Self = TypeVar('_Self', bound='IntegrityBase')


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
    def from_record(cls: Type[_Self], record: _Record,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        raise NotImplementedError("To be implemented by child class")

    @classmethod
    def make_manifest(cls, members: Mapping[_MemberName, _Member]) -> Manifest:
        """Make a :class:`.Manifest` for this integrity collection."""
        entries = [cls.make_manifest_entry(members[n]) for n in members]
        number_of_events_by_type = {
            etype: sum([e['number_of_events_by_type'][etype] for e in entries])
            for etype in EventType
        }
        return Manifest(
            entries=entries,
            number_of_events=sum([e['number_of_events'] for e in entries]),
            number_of_events_by_type=number_of_events_by_type,
            number_of_versions=sum([e['number_of_versions'] for e in entries]),
        )

    @classmethod
    def make_manifest_entry(cls, member: _Member) -> ManifestEntry:
        return ManifestEntry(key=member.manifest_name,
                             checksum=member.checksum,
                             number_of_events=member.manifest['number_of_events'],
                             number_of_events_by_type=member.manifest['number_of_events_by_type'],
                             number_of_versions=member.manifest['number_of_versions'])

    @property
    def checksum(self) -> str:
        """The checksum of this integrity collection."""
        assert self._checksum is not None
        return self._checksum

    @property
    def is_valid(self) -> bool:
        """Indicates whether or not this collection has a valid checksum."""
        return bool(self.checksum == self.calculate_checksum())

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
    def number_of_events(self) -> int:
        return self.record.domain.number_of_events

    @property
    def number_of_versions(self) -> int:
        return self.record.domain.number_of_versions

    @property
    def record(self) -> _Record:
        """The record associated with this collection."""
        assert self._record is not None
        return self._record

    def calculate_checksum(self) -> str:
        return calculate_checksum(self.manifest)

    def extend_manifest(self, member: _Member) -> None:
        entry = self.make_manifest_entry(member)
        self.manifest['entries'].append(entry)
        self.manifest['number_of_versions'] += entry['number_of_versions']
        self.manifest['number_of_events'] += entry['number_of_events']
        for key in self.manifest['number_of_events_by_type']:
             self.manifest['number_of_events_by_type'][key] += entry['number_of_events_by_type'][key]
        # print(self, type(self), self.manifest)
        self.update_checksum()

    def iter_members(self) -> Iterable[_Member]:
        return [self.members[name] for name in self.members]

    def update_checksum(self) -> None:
        """Set the checksum for this record."""
        self._checksum = self.calculate_checksum()

    def update_or_extend_manifest(self, member: _Member, checksum: str) \
            -> None:
        """Update the checksum on a manifest entry, or add a new entry."""
        found = False
        for entry in self.manifest['entries']:
            # Update existing manifest entry.
            if entry['key'] == member.integrity.manifest_name:
                entry['checksum'] = checksum
                found = True
                break
        if not found:   # New manifest entry.
            self.extend_manifest(member.integrity)


class IntegrityEntryBase(IntegrityBase[str, _Record, None, None]):
    record_type: Type[_Record]


class IntegrityEntry(IntegrityEntryBase[R.RecordEntry]):
    """Integrity concept for a single entry in the record."""

    record_type = R.RecordEntry

    @classmethod
    def from_record(cls: Type[_Self], record: R.RecordEntry,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        """Generate an :class:`.IntegrityEntry` from a :class:`.RecordEntry."""
        print('IntegrityEntry from_record::', record.key, record.stream)
        if calculate_new_checksum:
            checksum = calculate_checksum(record.stream)
        return cls(name=record.key, record=record, checksum=checksum)

    # This is redefined since the entry has no manifest; the record entry is
    # used instead.
    def calculate_checksum(self) -> str:
        return calculate_checksum(self.record.stream)


class IntegrityMetadata(IntegrityEntryBase[R.RecordMetadata]):
    """Integrity entry for a metadata bitstream in the record."""

    record_type = R.RecordMetadata

    @classmethod
    def from_record(cls: Type[_Self], record: R.RecordMetadata,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        if calculate_new_checksum:
            checksum = calculate_checksum(record.stream)
        return cls(name=record.key, record=record, checksum=checksum)

    # This is redefined since the entry has no manifest; the record entry is
    # used instead.
    def calculate_checksum(self) -> str:
        return calculate_checksum(self.record.stream)


class IntegrityListing(IntegrityEntryBase[R.RecordListing]):

    record_type = R.RecordListing

    @classmethod
    def from_record(cls: Type[_Self], record: R.RecordListing,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        """Generate an :class:`.IntegrityListing` from a :class:`.RecordListing."""
        if calculate_new_checksum:
            checksum = calculate_checksum(record.stream)
        return cls(name=record.key, record=record, checksum=checksum)

    # This is redefined since the entry has no manifest; the record entry is
    # used instead.
    def calculate_checksum(self) -> str:
        return calculate_checksum(self.record.stream)



class IntegrityListingDay(IntegrityBase[date,
                                        R.RecordListingDay,
                                        ListingIdentifier,
                                        IntegrityListing]):
    """Integrity collection of listings for a single day."""

    @classmethod
    def make_manifest_entry(cls, member: IntegrityListing) -> ManifestEntry:
        assert isinstance(member.record.domain, Listing)
        return ManifestEntry(key=member.manifest_name,
                             checksum=member.checksum,
                             size_bytes=member.record.stream.size_bytes,
                             mime_type=member.record.stream.content_type.mime_type,
                             number_of_versions=0,
                             number_of_events=len(member.record.domain.events),
                             number_of_events_by_type=member.record.domain.number_of_events_by_type)

    @property
    def manifest_name(self) -> str:
        """The name to use for this record in a parent manifest."""
        return self.name.isoformat()

    @classmethod
    def from_record(cls: Type[_Self], record: R.RecordListingDay,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True) -> _Self:
        """
        Generate an :class:`.IntegrityListing` from a :class:`.RecordListing`.
        """
        members = {name: IntegrityListing.from_record(record.members[name])
                   for name in record.members}
        # members = {
        #     record.listing.name: IntegrityEntry.from_record(record.listing)
        # }
        manifest = cls.make_manifest(members)
        if calculate_new_checksum:
            checksum = calculate_checksum(manifest)
        assert not isinstance(checksum, bool)
        return cls(record.name, members=members, manifest=manifest,
                   checksum=checksum)


class IntegrityListingMonth(IntegrityBase[YearAndMonth,
                                          R.RecordListingMonth,
                                          date,
                                          IntegrityListingDay]):
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
                                         R.RecordListingYear,
                                         YearAndMonth,
                                         IntegrityListingMonth]):
    """Integrity collection of listings for a single year."""

    @property
    def year(self) -> Year:
        """The numeric year represented by this collection."""
        return self.name


class IntegrityListings(IntegrityBase[str,
                                      R.RecordListings,
                                      Year,
                                      IntegrityListingYear]):
    """Integrity collection of all listings."""


def _checksum_from_manifest(manifest: Manifest, key: str) -> Optional[str]:
    for entry in manifest['entries']:
        if entry['key'] == key:
            return entry['checksum']
    raise KeyError(f'Not found: {key}')


class IntegrityVersion(IntegrityBase[VersionedIdentifier,
                                     R.RecordVersion,
                                     str,
                                     IntegrityEntry]):
    """Integrity collection for an e-print version."""

    @classmethod
    def from_record(cls: Type[_Self], version: R.RecordVersion,
                    checksum: Optional[str] = None,
                    calculate_new_checksum: bool = True,
                    manifest: Optional[Manifest] = None) -> _Self:
        """
        Get an :class:`.IntegrityVersion` from a :class:`.RecordVersion`.

        Parameters
        ----------
        version : :class:`.RecordVersion`
            The record for which this integrity object is to be generated.
        checksum : str or None
        manifest : dict
            If provided, checksum values for member files will be retrieved
            from this manifest. Otherwise they will be calculated from the
            file content.
        calculate_new_checksum : bool
            If ``True``, a new checksum will be calculated from the manifest.

        Returns
        -------
        :class:`.IntegrityVersion`

        """
        calculate_new_checksum_for_members = bool(manifest is None)

        render_checksum = _checksum_from_manifest(manifest, R.RecordVersion.make_key(version.identifier, version.render.domain.filename)) if manifest else None
        source_checksum = _checksum_from_manifest(manifest, R.RecordVersion.make_key(version.identifier, version.source.domain.filename)) if manifest else None

        print(version.render.__dict__)
        print(version.render.stream.__dict__)
        members = IntegrityEntryMembers(
            metadata=IntegrityEntry.from_record(version.metadata),
            render=IntegrityEntry.from_record(
                version.render,
                checksum=render_checksum,
                calculate_new_checksum=calculate_new_checksum_for_members
            ),
            source=IntegrityEntry.from_record(
                version.source,
                checksum=source_checksum,
                calculate_new_checksum=calculate_new_checksum_for_members
            )
        )

        manifest = cls.make_manifest(members)
        if calculate_new_checksum:
            checksum = calculate_checksum(manifest)
        return cls(version.identifier, record=version, members=members,
                   manifest=manifest, checksum=checksum)

    @classmethod
    def make_manifest(cls, members: Mapping[str, IntegrityEntry]) -> Manifest:
        """Make a :class:`.Manifest` for this integrity collection."""
        return Manifest(
            entries=[cls.make_manifest_entry(members[n]) for n in members],
            number_of_events=0,
            number_of_events_by_type={},
            number_of_versions=1
        )

    @classmethod
    def make_manifest_entry(cls, member: IntegrityEntry) -> ManifestEntry:
        return ManifestEntry(
            key=member.manifest_name,
            checksum=member.checksum,
            size_bytes=member.record.stream.size_bytes,
            mime_type=member.record.stream.content_type.mime_type
        )

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
                                    R.RecordEPrint,
                                    VersionedIdentifier,
                                    IntegrityVersion]):
    """Integrity collection for an :class:`.EPrint`."""

    @classmethod
    def make_manifest_entry(cls, member: IntegrityVersion) -> ManifestEntry:
        return ManifestEntry(key=member.manifest_name,
                             checksum=member.checksum,
                             number_of_versions=1,
                             number_of_events=0,
                             number_of_events_by_type={})


class IntegrityDay(IntegrityBase[date,
                                 R.RecordDay,
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
                                   R.RecordMonth,
                                   date,
                                   IntegrityDay]):
    """
    Integrity collection for e-prints associated with a single month.

    Specifically, this includes all versions of e-prints the first version of
    which was announced in this month.
    """

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


class IntegrityYear(IntegrityBase[Year,
                                  R.RecordYear,
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
                                     R.RecordEPrints,
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
                              R.Record,
                              str,
                              Union[IntegrityEPrints, IntegrityListings]]):
    """Global integrity collection."""


