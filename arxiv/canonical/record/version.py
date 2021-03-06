import datetime
from typing import Callable, Dict, IO, Iterable, Optional

from .core import RecordBase, RecordEntry, RecordEntryMembers, RecordStream, \
    D, Year, YearMonth
from .file import RecordFile
from .metadata import RecordMetadata


class RecordVersion(RecordBase[D.VersionedIdentifier,
                               str,
                               RecordEntry,
                               D.Version]):
    """
    A collection of serialized components that make up a version record.

    A version record is comprised of (1) a metadata record, (2) a source
    package, containing the original content provided by the submitter, and (3)
    a canonical rendering of the version (e.g. in PDF format).

    The key prefix structure for an version record is:

    ```
    e-prints/<YYYY>/<MM>/<arXiv ID>/v<version>/
    ```

    Where ``YYYY`` is the year and ``MM`` the month during which the first
    version of the e-print was announced.

    Sub-keys are:

    - Metadata record: ``<arXiv ID>v<version>.json``
    - Source package: ``<arXiv ID>v<version>.tar``
    - PDF: ``<arXiv ID>v<version>.render``
    - Manifest: ``<arXiv ID>v<version>.manifest.json``

    """

    @classmethod
    def from_domain(cls, version: D.Version,
                    dereferencer: Callable[[D.URI], IO[bytes]],
                    metadata: Optional[RecordMetadata] = None) -> 'RecordVersion':
        """Serialize an :class:`.Version` to an :class:`.RecordVersion`."""
        if version.source is None:
            raise ValueError('Source is missing')
        if version.announced_date_first is None:
            raise ValueError('First announcement date not set')

        # Dereference the bitstreams, wherever they happen to live.
        source_content = dereferencer(version.source.ref)
        format_content = {fmt: dereferencer(cf.ref)
                          for fmt, cf in version.formats.items()}

        source_key = RecordVersion.make_key(version.identifier,
                                            version.source.filename)
        format_keys = {
            fmt: RecordVersion.make_key(version.identifier, cf.filename)
            for fmt, cf in version.formats.items()
        }

        source = RecordFile(
            key=source_key,
            stream=RecordStream(
                domain=version.source,
                content=source_content,
                content_type=version.source.content_type,
                size_bytes=version.source.size_bytes,
            ),
            domain=version.source
        )

        formats = {
            fmt.value: RecordFile(
                key=format_keys[fmt],
                stream=RecordStream(
                    domain=cf,
                    content=format_content[fmt],
                    content_type=cf.content_type,
                    size_bytes=cf.size_bytes,
                ),
                domain=cf
            ) for fmt, cf in version.formats.items()
        }
        if version.render:
            render_content = dereferencer(version.render.ref)
            render_key = RecordVersion.make_key(version.identifier,
                                                version.render.filename)
            version.render.ref = render_key
            formats['render'] = RecordFile(
                key=render_key,
                stream=RecordStream(
                    domain=version.render,
                    content=render_content,
                    content_type=version.render.content_type,
                    size_bytes=version.render.size_bytes,
                ),
                domain=version.render
            )

        if metadata is None:
            metadata = RecordMetadata.from_domain(version)

        # From now on we refer to bitstreams with canonical URIs.
        version.source.ref = source_key
        for fmt, cf in version.formats.items():
            cf.ref = format_keys[fmt]

        return RecordVersion(
            version.identifier,
            members=RecordEntryMembers(
                metadata=metadata,
                source=source,
                **formats
            ),
            domain=version
        )

    @classmethod
    def make_key(cls, identifier: D.VersionedIdentifier,
                 filename: Optional[str] = None) -> D.Key:
        if filename is None:
            return RecordMetadata.make_key(identifier)
        return D.Key(f'{cls.make_prefix(identifier)}/{filename}')

    @classmethod
    def make_manifest_key(cls, ident: D.VersionedIdentifier) -> D.Key:
        date_part = f'e-prints/{ident.year}/{str(ident.month).zfill(2)}'
        if ident.is_old_style:
            return D.Key(f'{date_part}/{ident.category_part}/{ident.numeric_part}/{ident.numeric_part}.manifest.json')
        return D.Key(f'{date_part}/{ident.arxiv_id}/{ident}.manifest.json')

    @classmethod
    def make_prefix(cls, ident: D.VersionedIdentifier) -> str:
        """
        Make a key prefix for an e-print record.

        Parameters
        ----------
        date : datetime.date
            The day on which the first version of the e-print was announced.
        ident : str
            arXiv identifier

        Returns
        -------
        str

        """
        date_part = f'e-prints/{ident.year}/{str(ident.month).zfill(2)}'
        if ident.is_old_style:
            return (f'{date_part}/{ident.category_part}/{ident.numeric_part}/'
                    f'v{ident.version}')
        return f'{date_part}/{ident.arxiv_id}/v{ident.version}'

    @property
    def identifier(self) -> D.VersionedIdentifier:
        return self.name

    @property
    def metadata(self) -> RecordMetadata:
        """JSON document containing canonical e-print metadata."""
        assert 'metadata' in self.members
        member = self.members['metadata']
        assert isinstance(member, RecordMetadata)
        return member

    @property
    def render(self) -> Optional[RecordEntry]:
        """Canonical PDF for the e-print."""
        if 'render' in self.members:
            return self.members['render']
        return None

    @property
    def formats(self) -> Dict[D.ContentType, RecordEntry]:
        return {D.ContentType(fmt): entry
                for fmt, entry in self.members.items()
                if fmt not in ['metadata', 'source', 'render']}

    @property
    def source(self) -> RecordEntry:
        """Gzipped tarball containing the e-print source."""
        assert 'source' in self.members
        return self.members['source']

    def instance_to_domain(self) -> D.Version:
        """Deserialize an :class:`.RecordVersion` to an :class:`.Version`."""
        version = self.metadata.to_domain(self.metadata.stream)
        if version.source is None or version.render is None:
            raise ValueError('Failed to to_domain source or render metadata')
        return version


class RecordEPrint(RecordBase[D.Identifier,
                              D.VersionedIdentifier,
                              RecordVersion,
                              D.EPrint]):
    @classmethod
    def make_key(cls, idn: D.Identifier) -> D.Key:
        """
        Make a key prefix for an e-print record.

        Parameters
        ----------
        idn : str
            arXiv identifier

        Returns
        -------
        str

        """
        return D.Key(f'e-prints/{idn.year}/{str(idn.month).zfill(2)}/{idn}')

    @classmethod
    def make_manifest_key(cls, ident: D.Identifier) -> D.Key:
        """
        Make a key for an e-print manifest.

        Returns
        -------
        str

        """
        return D.Key(f'{cls.make_key(ident)}.manifest.json')


class RecordDay(RecordBase[datetime.date,
                           D.Identifier,
                           RecordEPrint,
                           D.EPrintDay]):
    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> D.Key:
        """
        Make a key for a daily e-print manifest.

        Returns
        -------
        str

        """
        return D.Key(date.strftime('e-prints/%Y/%m/%Y-%m-%d.manifest.json'))


class RecordMonth(RecordBase[YearMonth,
                             datetime.date,
                             RecordDay,
                             D.EPrintMonth]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> D.Key:
        """
        Make a key for a monthly e-print manifest.

        Returns
        -------
        str

        """
        y, m = year_and_month
        return D.Key(f'e-prints/{y}/{y}-{str(m).zfill(2)}.manifest.json')


class RecordYear(RecordBase[Year,
                            YearMonth,
                            RecordMonth,
                            D.EPrintYear]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> D.Key:
        """
        Make a key for a yearly e-print manifest.

        Returns
        -------
        str

        """
        return D.Key(f'e-prints/{year}.manifest.json')


class RecordEPrints(RecordBase[str, Year, RecordYear, D.AllEPrints]):
    @classmethod
    def make_manifest_key(cls, _: str) -> D.Key:
        """
        Make a key for all e-print manifest.

        Returns
        -------
        str

        """
        return D.Key(f'e-prints.manifest.json')