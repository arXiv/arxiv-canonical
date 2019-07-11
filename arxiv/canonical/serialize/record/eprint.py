"""
"""
import io
from json import dumps, loads, load
from typing import NamedTuple, List, IO, Iterator, Tuple, Optional, Dict
from datetime import datetime, date

from ...domain import EPrint
from ..encoder import CanonicalJSONEncoder
from ..decoder import CanonicalJSONDecoder
from .base import BaseEntry, IEntry, checksum, ChecksumError


class MetadataEntry(BaseEntry):
    content_type = 'application/json'

    @staticmethod
    def make_key(key_prefix: str, arxiv_id: str, version: str) -> str:
        return '/'.join([key_prefix, f'{arxiv_id}v{version}.json'])


class SourceEntry(BaseEntry):
    content_type = 'application/gzip'

    @staticmethod
    def make_key(key_prefix: str, arxiv_id: str, version: str) -> str:
        return '/'.join([key_prefix, f'{arxiv_id}v{version}.tar.gz'])


class PDFEntry(BaseEntry):
    content_type = 'application/pdf'

    @staticmethod
    def make_key(key_prefix: str, arxiv_id: str, version: str) -> str:
        return '/'.join([key_prefix, f'{arxiv_id}v{version}.pdf'])


class ManifestEntry(BaseEntry):
    content_type = 'application/json'

    @staticmethod
    def make_key(key_prefix: str, arxiv_id: str, version: str) -> str:
        return '/'.join([key_prefix, f'{arxiv_id}v{version}.manifest.json'])


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

    metadata: MetadataEntry
    """JSON document containing canonical e-print metadata."""

    source: SourceEntry
    """Gzipped tarball containing the e-print source."""

    pdf: PDFEntry
    """Canonical PDF for the e-print."""

    manifest: ManifestEntry
    """JSON document containing checksums for the metadata, source, and PDF."""

    def __iter__(self) -> Iterator[Tuple[str, IEntry]]:
        yield self.metadata.key, self.metadata
        yield self.source.key, self.source
        yield self.pdf.key, self.pdf
        yield self.manifest.key, self.manifest

    @staticmethod
    def key_prefix(year: int, month: int, arxiv_id: str, version: str) -> str:
        """
        Make a key prefix for an e-print record.

        Parameters
        ----------
        year : int
            The year during which the first version of the e-print was
            announced.
        month : int
            The month during which the first version of the e-print was
            announced.
        arxiv_id : str
            arXiv identifier (without version affix).
        version : str
            The numeric version of the e-print.

        Returns
        -------
        str

        """
        return '/'.join([
            'e-prints', str(year), str(month).zfill(2), arxiv_id, f'v{version}'
        ])


def _validate_checksum(key: str, content: IO[bytes], manifest: Dict[str, str]) -> None:
    calculated = checksum(content)
    if calculated != manifest[key]:
        raise ChecksumError(f'{key} has non-matching checksum; expected'
                            f' {manifest[key]}, got {calculated}')

def deserialize(record: EPrintRecord, validate: bool = True) -> EPrint:
    """Deserialize an :class:`.EPrintRecord` to an :class:`.EPrint`."""
    metadata = load(record.metadata.content, cls=CanonicalJSONDecoder)
    source = metadata.source_package.with_content(record.source.content)
    pdf = metadata.pdf.with_content(record.pdf.content)
    if validate:    # Compare calculated checksums to the manifest.
        manifest = load(record.manifest.content)
        _validate_checksum(record.metadata.key, record.metadata.content,
                           manifest)
        _validate_checksum(record.pdf.key, record.pdf.content,
                           manifest)
        _validate_checksum(record.source.key, record.source.content,
                           manifest)
    return metadata.with_files(source_package=source, pdf=pdf)


def serialize(eprint: EPrint, prefix: Optional[str] = None) -> EPrintRecord:
    """Serialize an :class:`.EPrint` to an :class:`.EPrintRecord`."""
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    if prefix is None:
        prefix = EPrintRecord.key_prefix(eprint.arxiv_id.year,
                                         eprint.arxiv_id.month,
                                         str(eprint.arxiv_id),
                                         eprint.version)
    metadata = _serialize_metadata(eprint, prefix)
    source = _serialize_source(eprint, prefix)
    pdf = _serialize_pdf(eprint, prefix)
    manifest = _serialize_manifest(eprint, metadata, source, pdf, prefix)
    return EPrintRecord(metadata=metadata,
                        source=source,
                        pdf=pdf,
                        manifest=manifest)

def _serialize_metadata(eprint: EPrint, prefix: str) -> MetadataEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    metadata_json = dumps(eprint, cls=CanonicalJSONEncoder)
    metadata_content = io.BytesIO(metadata_json.encode('utf-8'))
    return MetadataEntry(key=MetadataEntry.make_key(prefix,
                                                    str(eprint.arxiv_id),
                                                    eprint.version),
                         content=metadata_content)

def _serialize_source(eprint: EPrint, prefix: str) -> SourceEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    return SourceEntry(key=SourceEntry.make_key(prefix,
                                                str(eprint.arxiv_id),
                                                eprint.version),
                       content=eprint.source_package.content)

def _serialize_pdf(eprint: EPrint, prefix: str) -> PDFEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    return PDFEntry(key=PDFEntry.make_key(prefix,
                                          str(eprint.arxiv_id),
                                          eprint.version),
                    content=eprint.pdf.content)


def _serialize_manifest(eprint: EPrint, metadata: MetadataEntry,
                        source: SourceEntry, pdf: PDFEntry,
                        prefix: str) -> ManifestEntry:
    if eprint.arxiv_id is None:
        raise ValueError('Record serialization requires announced e-prints')
    return ManifestEntry(
        key=ManifestEntry.make_key(
            prefix,
            str(eprint.arxiv_id),
            eprint.version
        ),
        content=io.BytesIO(dumps({
            metadata.key: metadata.checksum,
            source.key: source.checksum,
            pdf.key: pdf.checksum
        }).encode('utf-8'))
    )


