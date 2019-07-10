import io
from json import dumps
from typing import NamedTuple, List, IO, Iterator, Tuple
from datetime import datetime, date

from ...domain import EPrint
from ..encoder import CanonicalJSONEncoder
from .base import BaseEntry, BaseDailyEntry, IEntry, checksum


class BaseEPrintEntry(NamedTuple):
    year: int
    """The year in which the first version of the e-print was announced."""

    month: int
    """The month in which the first version of the e-print was announced."""

    content: IO
    """Raw content of the entry."""

    checksum: str
    """URL-safe base64-encoded MD5 hash of the entry content."""

    arxiv_id: str
    """The arXiv identifier of the e-print."""

    version: int
    """The version of the e-print."""

    @property
    def key(self) -> str:
        """Key for this entry relative to the e-print base key."""
        raise NotImplementedError('Must be implemented by child class')


class MetadataEntry(BaseEPrintEntry):
    content_type = 'application/json'

    @property
    def key(self) -> str:
        """Key for this entry relative to the e-print base key."""
        return f'{self.arxiv_id}v{self.version}.json'


class SourceEntry(BaseEPrintEntry):
    content_type = 'application/gzip'

    @property
    def key(self) -> str:
        """Key for this entry relative to the e-print base key."""
        return f'{self.arxiv_id}v{self.version}.tar.gz'


class PDFEntry(BaseEPrintEntry):
    content_type = 'application/pdf'

    @property
    def key(self) -> str:
        """Key for this entry relative to the e-print base key."""
        return f'{self.arxiv_id}v{self.version}.pdf'


class ManifestEntry(BaseEPrintEntry):
    content_type = 'application/json'

    @property
    def key(self) -> str:
        """Key for this entry relative to the e-print base key."""
        return f'{self.arxiv_id}v{self.version}.manifest.json'


class EPrintRecord(NamedTuple):
    """
    A collection of serialized components that make up an e-print record.

    An e-print record is comprised of (1) a metadata record, (2) a source
    package, containing the original content provided by the submitter, and (3)
    a canonical rendering of the e-print in PDF format. A manifest is also
    stored for each e-print, containing the keys for the resources above and a
    base-64 encoded MD5 hash of their binary content.

    The key prefix structure for an e-print record is:

    ```
    e-prints/<YYYY>/<MM>/<arXiv ID>/v<version>/
    ```

    Where ``YYYY`` is the year and ``MM`` the month during which the first
    version of the e-print was announced.

    Sub-keys are:

    - Metadata record: ``<arXiv ID>v<version>.json``
    - Source package: ``<arXiv ID>v<version>.tar.gz``
    - PDF: ``<arXiv ID>v<version>.pdf``
    - Manifest: ``<arXiv ID>v<version>.manifest.json``

    """
    year: int
    """The year in which the first version of the e-print was announced."""

    month: int
    """The month in which the first version of the e-print was announced."""

    arxiv_id: str
    """The arXiv identifier of the e-print."""

    version: int
    """The version of the e-print."""

    metadata: BaseEPrintEntry
    """JSON document containing canonical e-print metadata."""

    source: BaseEPrintEntry
    """Gzipped tarball containing the e-print source."""

    pdf: BaseEPrintEntry
    """Canonical PDF for the e-print."""

    manifest: BaseEPrintEntry
    """JSON document containing checksums for the metadata, source, and PDF."""

    @property
    def _base_key(self) -> str:
        return '/'.join(['e-prints',
                         str(self.year),
                         str(self.month).zfill(2),
                         self.arxiv_id,
                         f'v{self.version}'])

    def get_full_key(self, entry: IEntry) -> str:
        """Get the full key for a :class:`.BaseEntry` in this record."""
        return '/'.join([self._base_key, entry.key])

    def __iter__(self) -> Iterator[Tuple[str, IEntry]]:
        yield self.get_full_key(self.metadata), self.metadata
        yield self.get_full_key(self.source), self.source
        yield self.get_full_key(self.pdf), self.pdf
        yield self.get_full_key(self.manifest), self.manifest


def serialize(eprint: EPrint) -> EPrintRecord:
    """Serialize an :class:`.EPrint` to an :class:`.EPrintRecord`."""
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    metadata = _serialize_metadata(eprint)
    source = _serialize_source(eprint)
    pdf = _serialize_pdf(eprint)
    manifest = _serialize_manifest(eprint, metadata, source, pdf)
    return EPrintRecord(year=eprint.arxiv_id.year,
                        month=eprint.arxiv_id.month,
                        arxiv_id=str(eprint.arxiv_id),
                        version=eprint.version,
                        metadata=metadata,
                        source=source,
                        pdf=pdf,
                        manifest=manifest)


def _serialize_metadata(eprint: EPrint) -> MetadataEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    metadata_json = dumps(eprint, cls=CanonicalJSONEncoder)
    metadata_content = io.BytesIO(metadata_json.encode('utf-8'))
    return MetadataEntry(year=eprint.arxiv_id.year,
                         month=eprint.arxiv_id.month,
                         arxiv_id=str(eprint.arxiv_id),
                         version=eprint.version,
                         content=metadata_content,
                         checksum=checksum(metadata_content))

def _serialize_source(eprint: EPrint) -> SourceEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    return SourceEntry(year=eprint.arxiv_id.year,
                       month=eprint.arxiv_id.month,
                       arxiv_id=str(eprint.arxiv_id),
                       version=eprint.version,
                       content=eprint.source_package.content,
                       checksum=eprint.source_package.checksum)

def _serialize_pdf(eprint: EPrint) -> PDFEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    return PDFEntry(year=eprint.arxiv_id.year,
                    month=eprint.arxiv_id.month,
                    arxiv_id=str(eprint.arxiv_id),
                    version=eprint.version,
                    content=eprint.pdf.content,
                    checksum=eprint.pdf.checksum)


def _serialize_manifest(eprint: EPrint, metadata: MetadataEntry,
                        source: SourceEntry, pdf: PDFEntry) -> ManifestEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    manifest_content = io.BytesIO(dumps({
        metadata.key: metadata.checksum,
        source.key: source.checksum,
        pdf.key: pdf.checksum
    }).encode('utf-8'))
    return ManifestEntry(year=eprint.arxiv_id.year,
                         month=eprint.arxiv_id.month,
                         arxiv_id=str(eprint.arxiv_id),
                         version=eprint.version,
                         content=manifest_content,
                         checksum=checksum(manifest_content))


