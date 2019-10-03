
from datetime import date
from typing import Optional, Type
from .core import (IntegrityBase, IntegrityEntryBase, D, R, _Self,
                   Year, YearAndMonth, calculate_checksum)
from .manifest import ManifestEntry, Manifest


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
                                        D.ListingIdentifier,
                                        IntegrityListing]):
    """Integrity collection of listings for a single day."""

    @classmethod
    def make_manifest_entry(cls, member: IntegrityListing) -> ManifestEntry:
        assert isinstance(member.record.domain, D.Listing)
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