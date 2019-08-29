"""
Serializers for low-level elements of the canonical record.

Specifically, this maps concepts in :mod:`.domain` to low-level elements in
:mod:`arxiv.canonical.record` and visa-versa.
"""

from io import BytesIO
from json import dumps, load
from typing import Tuple, IO

from ..domain import Version, ContentType, Listing, CanonicalFile
from ..record import RecordEntry, RecordVersion, RecordMetadata, \
    RecordEntryMembers, RecordListing
from .decoder import CanonicalDecoder
from .encoder import CanonicalEncoder


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
        content_type = ContentType.json
        return RecordEntry(
            domain=CanonicalFile(
                created=version.updated_date,
                modified=version.updated_date,
                size_bytes=size_bytes,
                content_type=content_type,
                content=content
            ),
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
            version.versioned_identifier,
            members=RecordEntryMembers(
                metadata=MetadataSerializer.serialize(version),
                source=RecordEntry(
                    domain=version.source,
                    key=RecordVersion.make_key(version.versioned_identifier,  # pylint: disable=no-member
                                               version.source.filename),
                    content=version.source.content,
                    content_type=version.source.content_type,
                    size_bytes=version.source.size_bytes
                ),
                render=RecordEntry(
                    domain=version.render,
                    key=RecordVersion.make_key(version.versioned_identifier,  # pylint: disable=no-member
                                               version.render.filename),
                    content=version.render.content,
                    content_type=version.render.content_type,
                    size_bytes=version.render.size_bytes
                ),
            )
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
        content_type = ContentType.json
        return RecordListing(
            listing.date,
            members=RecordEntryMembers(
                listing=RecordEntry(
                    domain=CanonicalFile(
                        created=listing.start_datetime,
                        modified=listing.end_datetime,
                        size_bytes=size_bytes,
                        content_type=content_type,
                        content=content
                    ),
                    key=RecordListing.make_key(listing.date),
                    content=content,
                    content_type=ContentType.json,
                    size_bytes=size_bytes
                )
            )
        )

    @staticmethod
    def _encode(listing: Listing) -> Tuple[IO[bytes], int]:
        content = dumps(listing, cls=CanonicalEncoder).encode('utf-8')
        return BytesIO(content), len(content)







