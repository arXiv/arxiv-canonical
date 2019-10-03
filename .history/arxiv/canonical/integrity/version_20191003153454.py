
from datetime import date
from typing import Mapping, Optional, Type

from ..manifest import ManifestEntry, Manifest, checksum_from_manifest

from .core import (IntegrityBase, IntegrityEntryBase, IntegrityEntryMembers,
                   IntegrityEntry, D, R, _Self, Year, Month, YearMonth,
                   calculate_checksum)
from .metadata import IntegrityMetadata


class IntegrityVersion(IntegrityBase[D.VersionedIdentifier,
                                     R.RecordVersion,
                                     str,
                                     Union[IntegrityEntry, IntegrityMetadata]]):
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

        render_checksum = checksum_from_manifest(manifest, R.RecordVersion.make_key(version.identifier, version.render.domain.filename)) if manifest else None
        source_checksum = checksum_from_manifest(manifest, R.RecordVersion.make_key(version.identifier, version.source.domain.filename)) if manifest else None

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


class IntegrityEPrint(IntegrityBase[D.Identifier,
                                    R.RecordEPrint,
                                    D.VersionedIdentifier,
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
                                 D.Identifier,
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


class IntegrityMonth(IntegrityBase[YearMonth,
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
                                  YearMonth,
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