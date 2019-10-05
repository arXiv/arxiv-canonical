"""
Defines how the canonical record is represented in a key-binary system.

The bulk of this module is concerned with how keys for records and record
manifests are generated.

Classes in this module are largely isomorphic to those in :mod:`.domain`.
:class:`.RecordEntry` represents content at the bitstream level, e.g. a file
containing a listings document or a render PDF. Collections of entries are
based on :class:`RecordBase`, and are composed hierarchically with the apex
at :class:`.Record`.
"""
import datetime
import os
from abc import ABC
from io import BytesIO
from json import dumps, load
from typing import NamedTuple, List, IO, Iterator, Tuple, Optional, Dict, \
    Callable, Iterable, MutableMapping, Mapping, Generic, Type, TypeVar, \
    Union, Any

from .serialize.decoder import CanonicalDecoder
from .serialize.encoder import CanonicalEncoder

from . import domain as D
from .domain import Version, Listing, Identifier, VersionedIdentifier, \
    ContentType, CanonicalFile, ListingIdentifier, ListingDay, ListingMonth, \
    ListingYear, AllListings, EPrint, EPrintDay, EPrintMonth, EPrintYear, \
    AllEPrints, Canon, URI, CanonicalBase, Key
from .util import GenericMonoDict

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class RecordStream(NamedTuple):
    """A single bitstream in the record."""

    domain: CanonicalFile

    content: Optional[IO[bytes]]
    """Raw content of the entry."""

    content_type: ContentType
    """MIME-type of the content."""

    size_bytes: int
    """Size of ``content`` in bytes."""

    @property
    def created(self) -> datetime.datetime:
        return self.domain.created


class RecordEntryMembers(GenericMonoDict[str, 'RecordEntry']):
    """
    A dict that returns only :class: `.RecordEntry` instances.

    Consistent with ``Mapping[str, RecordEntry]``.
    """
    def __getitem__(self, key: str) -> 'RecordEntry':
        value = dict.__getitem__(self, key)
        assert isinstance(value, RecordEntry)
        return value


_EDomain = TypeVar('_EDomain', bound=CanonicalBase)
_Self = TypeVar('_Self', bound='RecordEntry')


class RecordEntry(Generic[_EDomain]):
    """
    An entry in the canonical record.

    Comprised of a :class:`.RecordStream` and a domain representation of the
    entry (i.e. the application-level interpretation of the stream).
    """

    key: Key
    """Full key (path) at which the entry is stored."""
    domain: _EDomain
    stream: RecordStream

    def __init__(self, key: Key, stream: RecordStream, domain: _EDomain) \
            -> None:
        self.key = key
        self.domain = domain
        self.stream = stream

    @property
    def name(self) -> str:
        fname = os.path.split(self.key)[1]
        return os.path.splitext(fname)[0]

    @classmethod
    def from_domain(cls: Type[_Self], d: _EDomain,
                    callbacks: Iterable[D.Callback] = ()) -> _Self:
        raise NotImplementedError("Must be implemented by child class")

    @classmethod
    def to_domain(cls, stream: RecordStream,
                  callbacks: Iterable[D.Callback] = ()) -> _EDomain:
        raise NotImplementedError("Must be implemented by child class")


class RecordFile(RecordEntry[CanonicalFile]):
    """An entry that is handled as an otherwise-uninterpreted file."""


class RecordListing(RecordEntry[Listing]):
    """A listing entry."""

    @classmethod
    def from_domain(cls, listing: Listing,
                    callbacks: Iterable[D.Callback] = ()) -> 'RecordListing':
        """Serialize a :class:`.Listing`."""
        content, size_bytes = cls._encode(listing, callbacks=callbacks)
        key = cls.make_key(listing.identifier)
        return cls(
            key=key,
            stream=RecordStream(
                domain=CanonicalFile(
                    created=listing.start_datetime,
                    modified=listing.end_datetime,
                    size_bytes=size_bytes,
                    content_type=ContentType.json,
                    # content=content,
                    filename=key.filename,
                    ref=key
                ),
                content=content,
                content_type=ContentType.json,
                size_bytes=size_bytes
            ),
            domain=listing
        )

    @classmethod
    def from_stream(cls, key: Key, stream: RecordStream,
                    callbacks: Iterable[D.Callback] = ()) -> 'RecordListing':
        return cls(key=key, stream=stream,
                   domain=cls.to_domain(stream, callbacks=callbacks))

    @classmethod
    def make_key(cls, identifier: ListingIdentifier) -> Key:
        prefix = cls.make_prefix(identifier.date)
        value: str = identifier.date.strftime(
            f'{prefix}/%Y-%m-%d-{identifier.name}.json'
        )
        return Key(value)

    @classmethod
    def make_prefix(cls, date: datetime.date) -> str:
        return date.strftime(f'announcement/%Y/%m/%d')

    # @classmethod
    # def post_to_domain(cls, listing: Listing,
    #                    loader: Callable[[Key], IO[bytes]]) -> Listing:
    #     for event in listing.events:
    #         event.version = RecordVersion.post_to_domain(event.version, loader)
    #     return listing

    @classmethod
    def to_domain(cls, stream: RecordStream,
                  callbacks: Iterable[D.Callback] = ()) -> Listing:
        assert stream.content is not None
        listing = Listing.from_dict(load(stream.content), callbacks=callbacks)
        if stream.content.seekable:
            stream.content.seek(0)
        return listing

    @staticmethod
    def _encode(listing: Listing,
                callbacks: Iterable[D.Callback] = ()) -> Tuple[IO[bytes], int]:
        # for event in listing_data['events']:
        #     event['version']['source']['content'] = str(URI.make_canonical_uri(
        #         RecordVersion.make_key(
        #             VersionedIdentifier(event['version']['identifier']),
        #             event['version']['source']['filename']
        #         )
        #     ))
        #     event['version']['render']['content'] = str(URI.make_canonical_uri(
        #         RecordVersion.make_key(
        #             VersionedIdentifier(event['version']['identifier']),
        #             event['version']['render']['filename']
        #         )
        #     ))
        content = dumps(listing.to_dict(callbacks=callbacks)).encode('utf-8')
        return BytesIO(content), len(content)

    @property
    def created(self) -> datetime.datetime:
        return self.domain.start_datetime

    @property
    def name(self) -> str:
        return 'listing'


class RecordMetadata(RecordEntry[Version]):
    """An entry for version metadata."""

    @classmethod
    def make_key(cls, identifier: VersionedIdentifier) -> Key:
        return Key(f'{cls.make_prefix(identifier)}/{identifier}.json')

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
        return (f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/'
                f'{ident.arxiv_id}/v{ident.version}')

    @classmethod
    def from_domain(cls, version: Version,
                    callbacks: Iterable[D.Callback] = ()) -> 'RecordMetadata':
        content, size_bytes = cls._encode(version, callbacks=callbacks)
        content_type = ContentType.json
        key = cls.make_key(version.identifier)
        return RecordMetadata(
            key=key,
            stream=RecordStream(
                domain=CanonicalFile(
                    created=version.updated_date,
                    modified=version.updated_date,
                    size_bytes=size_bytes,
                    content_type=content_type,
                    # content=content,
                    ref=key,
                    filename=key.filename
                ),
                content=content,
                content_type=ContentType.json,
                size_bytes=size_bytes
            ),
            domain=version
        )

    @staticmethod
    def _encode(version: Version,
                callbacks: Iterable[D.Callback] = ()) -> Tuple[IO[bytes], int]:
        # version_data['source']['content'] = str(URI.make_canonical_uri(
        #     RecordVersion.make_key(
        #         VersionedIdentifier(version_data['identifier']),
        #         version_data['source']['filename']
        #     )
        # ))
        # version_data['render']['content'] = str(URI.make_canonical_uri(
        #     RecordVersion.make_key(
        #         VersionedIdentifier(version_data['identifier']),
        #         version_data['render']['filename']
        #     )
        # ))
        content = dumps(version.to_dict(callbacks=callbacks)).encode('utf-8')
        return BytesIO(content), len(content)

    @classmethod
    def to_domain(self, stream: RecordStream,
                  callbacks: Iterable[D.Callback] = ()) -> Version:
        assert stream.content is not None
        version = Version.from_dict(load(stream.content), callbacks=callbacks)
        if stream.content.seekable:
            stream.content.seek(0)
        return version  # RecordVersion.post_to_domain(version, load_content)

    @classmethod
    def from_stream(cls, key: Key, stream: RecordStream, callbacks: Iterable[D.Callback] = ()) -> 'RecordMetadata':
        return cls(key=key, stream=stream,
                   domain=cls.to_domain(stream, callbacks=callbacks))


# These TypeVars are used as placeholders in the generic RecordBase class,
# below. To learn more about TypeVars and Generics, see
# https://mypy.readthedocs.io/en/latest/generics.html
Name = TypeVar('Name')
MemberName = TypeVar('MemberName')
Member = TypeVar('Member', bound=Union['RecordBase', RecordEntry])
Domain = TypeVar('Domain')


class RecordBase(Generic[Name, MemberName, Member, Domain]):
    """
    Generic base class for record collections in this module.

    This produces a uniform protocol for record collections, while allowing
    name, member, and member name types to vary across collection subclasses.
    """

    def __init__(self, name: Name,
                 members: Mapping[MemberName, Member],
                 domain: Domain) -> None:
        """Register the name and members of this record instance."""
        self.name = name
        self.members = members
        self.domain = domain

    @classmethod
    def make_manifest_key(cls, name: Name) -> Key:  # pylint: disable=unused-argument
        """Generate a full key that can be used to store a manifest."""
        ...  # pylint: disable=pointless-statement ; this is a stub.


class RecordVersion(RecordBase[VersionedIdentifier,
                               str,
                               RecordEntry,
                               Version]):
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

    @classmethod
    def from_domain(cls, version: Version,
                    dereferencer: Callable[[D.URI], IO[bytes]],
                    metadata: Optional[RecordMetadata] = None,
                    callbacks: Iterable[D.Callback] = ()) -> 'RecordVersion':
        """Serialize an :class:`.Version` to an :class:`.RecordVersion`."""
        if version.source is None:
            raise ValueError('Source is missing')
        if version.render is None:
            raise ValueError('Render is missing')
        if version.announced_date_first is None:
            raise ValueError('First announcement date not set')

        if metadata is None:
            metadata = RecordMetadata.from_domain(version, callbacks=callbacks)

        return RecordVersion(
            version.identifier,
            members=RecordEntryMembers(
                metadata=metadata,
                source=RecordFile(
                    key=RecordVersion.make_key(version.identifier,  # pylint: disable=no-member
                                               version.source.filename),
                    stream=RecordStream(
                        domain=version.source,
                        content=None,
                        # content=version.source.content,
                        content_type=version.source.content_type,
                        size_bytes=version.source.size_bytes,
                    ),
                    domain=version.source
                ),
                render=RecordFile(
                    key=RecordVersion.make_key(version.identifier,  # pylint: disable=no-member
                                               version.render.filename),
                    stream=RecordStream(
                        domain=version.render,
                        content=None,
                        # content=version.render.content,
                        content_type=version.render.content_type,
                        size_bytes=version.render.size_bytes,
                    ),
                    domain=version.render
                )
            ),
            domain=version
        )

    @classmethod
    def make_key(cls, identifier: VersionedIdentifier,
                 filename: Optional[str] = None) -> Key:
        if filename is None:
            return RecordMetadata.make_key(identifier)
        return Key(f'{cls.make_prefix(identifier)}/{filename}')

    @classmethod
    def make_manifest_key(cls, ident: VersionedIdentifier) -> Key:
        return Key(f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/'
                   f'{ident.arxiv_id}/{ident}.manifest.json')

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
        return f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident.arxiv_id}/v{ident.version}'

    # @classmethod
    # def post_to_domain(cls, version: Version,
    #                    load_content: Callable[[Key], IO[bytes]]) -> Version:
    #     if isinstance(version.source.content, URI) \
    #             and version.source.content.is_canonical:
    #         version.source.content = load_content(
    #             Key(version.source.content.path.lstrip('/'))
    #         )

    #     if isinstance(version.render.content, URI) \
    #             and version.render.content.is_canonical:
    #         version.render.content = load_content(Key(version.render.content.path.lstrip('/')))
    #     return version

    @property
    def identifier(self) -> VersionedIdentifier:
        return self.name

    @property
    def metadata(self) -> RecordMetadata:
        """JSON document containing canonical e-print metadata."""
        assert 'metadata' in self.members
        member = self.members['metadata']
        assert isinstance(member, RecordMetadata)
        return member

    @property
    def render(self) -> RecordEntry:
        """Canonical PDF for the e-print."""
        assert 'render' in self.members
        return self.members['render']

    @property
    def source(self) -> RecordEntry:
        """Gzipped tarball containing the e-print source."""
        assert 'source' in self.members
        return self.members['source']

    def to_domain(self, callbacks: Iterable[D.Callback] = ()) -> Version:
        """Deserialize an :class:`.RecordVersion` to an :class:`.Version`."""
        version = self.metadata.to_domain(self.metadata.stream,
                                          callbacks=callbacks)
        if version.source is None or version.render is None:
            raise ValueError('Failed to to_domain source or render metadata')
        return version  # self.post_to_domain(version, load_content)


class RecordListingDay(RecordBase[datetime.date,
                                  ListingIdentifier,
                                  RecordListing,
                                  ListingDay]):

    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> Key:
        return Key(date.strftime('announcement/%Y/%m/%Y-%m-%d.manifest.json'))


class RecordListingMonth(RecordBase[YearMonth,
                                    datetime.date,
                                    RecordListing,
                                    ListingMonth]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> Key:
        """
        Make a key for a monthly listing manifest.

        Returns
        -------
        str

        """
        yr, month = year_and_month
        return Key(f'announcement/{yr}/{yr}-{str(month).zfill(2)}.manifest.json')


class RecordListingYear(RecordBase[Year,
                                   YearMonth,
                                   RecordListingMonth,
                                   ListingYear]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> Key:
        """
        Make a key for a yearly listing manifest.

        Returns
        -------
        str

        """
        return Key(f'announcement/{year}.manifest.json')


class RecordListings(RecordBase[str, Year, RecordListingYear, AllListings]):

    @classmethod
    def make_manifest_key(cls, _: str) -> Key:
        """
        Make a key for a root listing manifest.

        Returns
        -------
        str

        """
        return Key('announcement.manifest.json')


class RecordEPrint(RecordBase[Identifier,
                              VersionedIdentifier,
                              RecordVersion,
                              EPrint]):
    @classmethod
    def make_key(cls, ident: Identifier) -> Key:
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
        return Key(f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident}')

    @classmethod
    def make_manifest_key(cls, ident: Identifier) -> Key:
        """
        Make a key for an e-print manifest.

        Returns
        -------
        str

        """
        return Key(f'{cls.make_key(ident)}.manifest.json')


class RecordDay(RecordBase[datetime.date,
                           Identifier,
                           RecordEPrint,
                           EPrintDay]):
    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> Key:
        """
        Make a key for a daily e-print manifest.

        Returns
        -------
        str

        """
        return Key(date.strftime('e-prints/%Y/%m/%Y-%m-%d.manifest.json'))


class RecordMonth(RecordBase[YearMonth,
                             datetime.date,
                             RecordDay,
                             EPrintMonth]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> Key:
        """
        Make a key for a monthly e-print manifest.

        Returns
        -------
        str

        """
        year, month = year_and_month
        return Key(f'e-prints/{year}/{year}-{str(month).zfill(2)}.manifest.json')


class RecordYear(RecordBase[Year,
                            YearMonth,
                            RecordMonth,
                            EPrintYear]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> Key:
        """
        Make a key for a yearly e-print manifest.

        Returns
        -------
        str

        """
        return Key(f'e-prints/{year}.manifest.json')


class RecordEPrints(RecordBase[str, Year, RecordYear, AllEPrints]):
    @classmethod
    def make_manifest_key(cls, _: str) -> Key:
        """
        Make a key for all e-print manifest.

        Returns
        -------
        str

        """
        return Key(f'e-prints.manifest.json')


class Record(RecordBase[str,
                        str,
                        Union[RecordEPrints, RecordListings],
                        Canon]):
    @classmethod
    def make_manifest_key(cls, _: str) -> Key:
        """
        Make a key for global manifest.

        Returns
        -------
        str

        """
        return Key(f'global.manifest.json')

    @property
    def eprints(self) -> RecordEPrints:
        assert 'eprints' in self.members
        assert isinstance(self.members['eprints'], RecordEPrints)
        return self.members['eprints']

    @property
    def listings(self) -> RecordListings:
        assert 'listings' in self.members
        assert isinstance(self.members['listings'], RecordListings)
        return self.members['listings']