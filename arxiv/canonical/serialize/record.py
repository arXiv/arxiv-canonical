"""
"""
import datetime
from json import dumps, load
from io import BytesIO
from typing import NamedTuple, List, IO, Iterator, Tuple, Optional, Dict, \
    Callable, Iterable, MutableMapping, Mapping

from ..domain import Version, Listing, Identifier, VersionedIdentifier, \
    ContentType
from .encoder import CanonicalEncoder
from .decoder import CanonicalDecoder


class RecordEntry(NamedTuple):
    """A single bitstream in the record."""

    key: str

    content: IO[bytes]
    """Raw content of the entry."""

    content_type: ContentType
    """MIME-type of the content."""

    size_bytes: int


class RecordMetadata(RecordEntry):
    @classmethod
    def make_key(cls, identifier: VersionedIdentifier) -> str:
        return f'{cls.make_prefix(identifier)}/{identifier}.json'

    @classmethod
    def make_prefix(cls, ident: VersionedIdentifier) -> str:
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
        return f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident}'


class RecordVersion(NamedTuple):
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
    - Source package: ``<arXiv ID>v<version>.tar.gz``
    - PDF: ``<arXiv ID>v<version>.render``
    - Manifest: ``<arXiv ID>v<version>.manifest.json``

    """
    identifier: VersionedIdentifier

    metadata: RecordEntry
    """JSON document containing canonical e-print metadata."""

    render: RecordEntry
    """Canonical PDF for the e-print."""

    source: RecordEntry
    """Gzipped tarball containing the e-print source."""

    @classmethod
    def make_key(cls, identifier: VersionedIdentifier,
                 filename: Optional[str] = None) -> str:
        if filename is None:
            return RecordMetadata.make_key(identifier)  # pylint: disable=no-member
        return f'{cls.make_prefix(identifier)}/{filename}'

    @classmethod
    def make_prefix(cls, ident: VersionedIdentifier) -> str:
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
        return f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident}'


class RecordListing(NamedTuple):
    date: datetime.date
    listing: RecordEntry

    @classmethod
    def make_key(cls, date: datetime.date) -> str:
        return f'{cls.make_prefix(date)}/listing.json'

    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> str:
        return f'{cls.make_prefix(date)}.manifest.json'

    @classmethod
    def make_prefix(cls, date: datetime.date) -> str:
        return date.strftime(f'announcement/%Y/%m/%d')


class RecordListingRange(NamedTuple):
    start_date: datetime.date
    end_date: datetime.date
    listings: Iterable[RecordListing]


class RecordListingMonth(NamedTuple):
    year: int
    month: int
    days: Mapping[datetime.date, RecordListing]

    @classmethod
    def make_manifest_key(cls, year: int, month: int) -> str:
        """
        Make a key for a monthly listing manifest.

        Returns
        -------
        str

        """
        return f'announcement/{year}/{year}-{str(month).zfill(2)}.manifest.json'


class RecordListingYear(NamedTuple):
    year: int
    months: Mapping[Tuple[int, int], RecordListingMonth]

    @classmethod
    def make_manifest_key(cls, year: int) -> str:
        """
        Make a key for a yearly listing manifest.

        Returns
        -------
        str

        """
        return f'announcement/{year}.manifest.json'


class RecordAllListings(NamedTuple):
    years: Mapping[int, RecordListingYear]

    @classmethod
    def make_manifest_key(cls) -> str:
        """
        Make a key for a root listing manifest.

        Returns
        -------
        str

        """
        return 'announcement.manifest.json'


class RecordEPrint(NamedTuple):
    identifier: Identifier
    versions: Iterable[RecordVersion]

    @classmethod
    def make_key(cls, ident: Identifier) -> str:
        """
        Make a key prefix for an e-print record.

        Parameters
        ----------
        ident : str
            arXiv identifier

        Returns
        -------
        str

        """
        return f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident}'

    @classmethod
    def make_manifest_key(cls, ident: Identifier) -> str:
        """
        Make a key for an e-print manifest.

        Returns
        -------
        str

        """
        return f'{cls.make_key(ident)}.manifest.json'


class RecordDay(NamedTuple):
    date: datetime.date
    eprints: MutableMapping[Identifier, RecordEPrint]

    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> str:
        """
        Make a key for a daily e-print manifest.

        Returns
        -------
        str

        """
        return date.strftime('e-prints/%Y/%m/%Y-%m-%d.manifest.json')


class RecordMonth(NamedTuple):
    year: int
    month: int
    days: MutableMapping[datetime.date, RecordDay]

    @classmethod
    def make_manifest_key(cls, year: int, month: int) -> str:
        """
        Make a key for a monthly e-print manifest.

        Returns
        -------
        str

        """
        return f'e-prints/{year}/{year}-{str(month).zfill(2)}.manifest.json'


class RecordYear(NamedTuple):
    year: int
    months: Mapping[Tuple[int, int], RecordMonth]

    @classmethod
    def make_manifest_key(cls, year: int) -> str:
        """
        Make a key for a yearly e-print manifest.

        Returns
        -------
        str

        """
        return f'e-prints/{year}.manifest.json'


class RecordAllEPrints(NamedTuple):
    years: Mapping[int, RecordYear]


class Record(NamedTuple):
    eprints: RecordAllEPrints
    listings: RecordAllListings


class CanonicalRecord(NamedTuple):
    months: Iterable[RecordMonth]


class MetadataSerializer:
    @classmethod
    def deserialize(cls, record: RecordEntry) -> Version:
        version: Version = load(record.content, cls=CanonicalDecoder)
        if record.content.seekable:
            record.content.seek(0)
        return version

    @classmethod
    def serialize(cls, version: Version) -> RecordEntry:
        content, size_bytes = cls._encode(version)
        return RecordEntry(
            key=RecordMetadata.make_key(version.versioned_identifier),
            content=content,
            content_type=ContentType.json,
            size_bytes=size_bytes
        )

    @staticmethod
    def _encode(version: Version) -> Tuple[IO[bytes], int]:
        content = dumps(version, cls=CanonicalEncoder).encode('utf-8')
        return BytesIO(content), len(content)


class VersionSerializer:
    @classmethod
    def deserialize(cls, record: RecordVersion) -> Version:
        """Deserialize an :class:`.RecordVersion` to an :class:`.Version`."""
        version = MetadataSerializer.deserialize(record.metadata)
        if version.source is None or version.render is None:
            raise ValueError('Failed to deserialize source or render metadata')
        version = version.with_files(
            source=version.source.with_content(record.source.content),
            render=version.render.with_content(version.render.content)
        )
        return version

    @classmethod
    def serialize(cls, version: Version) -> RecordVersion:
        """Serialize an :class:`.Version` to an :class:`.RecordVersion`."""
        if version.identifier is None or version.version is None:
            raise ValueError('Version is missing identifier')
        if version.source is None or version.source.content is None:
            raise ValueError('Source is missing')
        if version.render is None or version.render.content is None:
            raise ValueError('Render is missing')
        if version.announced_date_first is None:
            raise ValueError('First announcement date not set')

        return RecordVersion(
            identifier=version.versioned_identifier,
            metadata=MetadataSerializer.serialize(version),
            source=RecordEntry(
                key=RecordVersion.make_key(version.versioned_identifier,  # pylint: disable=no-member
                                           version.source.filename),
                content=version.source.content,
                content_type=version.source.content_type,
                size_bytes=version.source.size_bytes
            ),
            render=RecordEntry(
                key=RecordVersion.make_key(version.versioned_identifier,  # pylint: disable=no-member
                                           version.render.filename),
                content=version.render.content,
                content_type=version.render.content_type,
                size_bytes=version.render.size_bytes
            ),
        )



class ListingSerializer:
    @classmethod
    def deserialize(cls, record: RecordListing) -> Listing:
        """Deserialize an :class:`.ListingEntry` to an :class:`.Listing`."""
        listing: Listing = load(record.listing.content, cls=CanonicalDecoder)
        if record.listing.content.seekable:
            record.listing.content.seek(0)
        return listing

    @classmethod
    def serialize(cls, listing: Listing) -> RecordListing:
        """Serialize a :class:`.Listing`."""
        content, size_bytes = cls._encode(listing)
        return RecordListing(
            date=listing.date,
            listing=RecordEntry(
                key=RecordListing.make_key(listing.date),
                content=content,
                content_type=ContentType.json,
                size_bytes=size_bytes
            )
        )

    @staticmethod
    def _encode(listing: Listing) -> Tuple[IO[bytes], int]:
        content = dumps(listing, cls=CanonicalEncoder).encode('utf-8')
        return BytesIO(content), len(content)







