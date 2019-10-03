"""Base classes and concepts for the integrity system."""

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
        assert isinstance(self.record.domain.number_of_events, int)
        return self.record.domain.number_of_events

    @property
    def number_of_versions(self) -> int:
        assert isinstance(self.record.domain.number_of_versions, int)
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