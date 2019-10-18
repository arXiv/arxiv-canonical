
from datetime import date
from typing import Dict, Mapping, Optional, Type, Union

from ..manifest import ManifestEntry, Manifest, checksum_from_manifest

from .core import (IntegrityBase, IntegrityEntryBase, IntegrityEntryMembers,
                   IntegrityEntry, D, R, _Self, Year, Month, YearMonth,
                   calculate_checksum, GenericMonoDict)
from .metadata import IntegrityMetadata

_VersionMember = Union[IntegrityEntry, IntegrityMetadata]


class IntegrityVersionMembers(GenericMonoDict[str, _VersionMember]):
    """Member mapping that supports IntegrityEntry and IntegrityMetadata."""

    def __getitem__(self, key: str) -> _VersionMember:
        value = dict.__getitem__(self, key)
        assert isinstance(value, (IntegrityEntry, IntegrityMetadata))
        return value


class IntegrityVersion(IntegrityBase[D.VersionedIdentifier,
                                     R.RecordVersion,
                                     str,
                                     _VersionMember]):
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
        render_checksum: Optional[str] = None
        source_checksum: Optional[str] = None
        format_checksums: Dict[D.ContentType, Optional[str]]
        if manifest:
            source_checksum = checksum_from_manifest(
                manifest,
                R.RecordVersion.make_key(
                    version.identifier,
                    version.source.domain.filename
                )
            )
            format_checksums = {
                fmt: checksum_from_manifest(
                    manifest,
                    R.RecordVersion.make_key(version.identifier,
                                             cf.domain.filename)
                ) for fmt, cf in version.formats.items()
            }
            if version.render:
                render_checksum = checksum_from_manifest(
                    manifest,
                    R.RecordVersion.make_key(
                        version.identifier,
                        version.render.domain.filename
                    )
                )
        formats = {
            fmt.value: IntegrityEntry.from_record(
                cf,
                checksum=format_checksums.get(fmt),
                calculate_new_checksum=calculate_new_checksum_for_members
            ) for fmt, cf in version.formats.items()
        }
        if version.render:
            formats['render'] = IntegrityEntry.from_record(
                version.render,
                checksum=render_checksum,
                calculate_new_checksum=calculate_new_checksum_for_members
            )
        members = IntegrityVersionMembers(
            metadata=IntegrityMetadata.from_record(version.metadata),
            source=IntegrityEntry.from_record(
                version.source,
                checksum=source_checksum,
                calculate_new_checksum=calculate_new_checksum_for_members
            ),
            **formats
        )
        manifest = cls.make_manifest(members)
        if calculate_new_checksum:
            checksum = calculate_checksum(manifest)
        return cls(version.identifier, record=version, members=members,
                   manifest=manifest, checksum=checksum)

    @classmethod
    def make_manifest(cls, members: Mapping[str, _VersionMember]) -> Manifest:
        """Make a :class:`.Manifest` for this integrity collection."""
        return Manifest(
            entries=[cls.make_manifest_entry(members[n]) for n in members],
            number_of_events=0,
            number_of_events_by_type={},
            number_of_versions=1
        )

    @classmethod
    def make_manifest_entry(cls, member: _VersionMember) -> ManifestEntry:
        return ManifestEntry(
            key=member.manifest_name,
            checksum=member.checksum,
            size_bytes=member.record.stream.size_bytes,
            mime_type=member.record.stream.content_type.mime_type
        )

    @property
    def metadata(self) -> IntegrityMetadata:
        assert isinstance(self.members['metadata'], IntegrityMetadata)
        return self.members['metadata']

    @property
    def render(self) -> Optional[IntegrityEntry]:
        if 'render' in self.members:
            assert isinstance(self.members['render'], IntegrityEntry)
            return self.members['render']
        return None

    @property
    def source(self) -> IntegrityEntry:
        assert isinstance(self.members['source'], IntegrityEntry)
        return self.members['source']

    @property
    def formats(self) -> Dict[D.ContentType, IntegrityEntry]:
        return {D.ContentType(fmt): cf for fmt, cf in self.members.items()
                if fmt not in ['metadata', 'source', 'render']
                and isinstance(cf, IntegrityEntry)}


class IntegrityEPrint(IntegrityBase[D.Identifier,
                                    R.RecordEPrint,
                                    D.VersionedIdentifier,
                                    IntegrityVersion]):
    """Integrity collection for an :class:`.EPrint`."""

    member_type = IntegrityVersion

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