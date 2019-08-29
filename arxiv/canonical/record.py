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
from io import BytesIO
from typing import NamedTuple, List, IO, Iterator, Tuple, Optional, Dict, \
    Callable, Iterable, MutableMapping, Mapping, Generic, TypeVar, Union

from .domain import Version, Listing, Identifier, VersionedIdentifier, \
    ContentType, CanonicalFile
from .util import GenericMonoDict

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class RecordEntry(NamedTuple):
    """A single bitstream in the record."""

    domain: CanonicalFile

    key: str
    """Full key (path) at which the entry is stored."""

    content: IO[bytes]
    """Raw content of the entry."""

    content_type: ContentType
    """MIME-type of the content."""

    size_bytes: int
    """Size of ``content`` in bytes."""

    @property
    def name(self) -> str:
        fname = os.path.split(self.key)[1]
        if 'listing' in fname:
            return 'listing'
        return os.path.splitext(fname)[0]


class RecordEntryMembers(GenericMonoDict[str, RecordEntry]):
    """
    A dict that returns only :class: `.RecordEntry` instances.

    Consistent with ``Mapping[str, RecordEntry]``.
    """
    def __getitem__(self, key: str) -> RecordEntry:
        value = dict.__getitem__(self, key)
        assert isinstance(value, RecordEntry)
        return value


class RecordMetadata(RecordEntry):
    """
    An entry for version metadata.

    Provides standardized key generation for metadata records.
    """

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
        return f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident.arxiv_id}/v{ident.version}'


# These TypeVars are used as placeholders in the generic RecordBase class,
# below. To learn more about TypeVars and Generics, see
# https://mypy.readthedocs.io/en/latest/generics.html
Name = TypeVar('Name')
MemberName = TypeVar('MemberName')
Member = TypeVar('Member', bound=Union['RecordBase', RecordEntry])


class RecordBase(Generic[Name, MemberName, Member]):
    """
    Generic base class for record collections in this module.

    This produces a uniform protocol for record collections, while allowing
    name, member, and member name types to vary across collection subclasses.
    """

    def __init__(self, name: Name,
                 members: Mapping[MemberName, Member]) -> None:
        """Register the name and members of this record instance."""
        self.name = name
        self.members = members

    @classmethod
    def make_manifest_key(cls, name: Name) -> str:  # pylint: disable=unused-argument
        """Generate a full key that can be used to store a manifest."""
        ...  # pylint: disable=pointless-statement ; this is a stub.


class RecordVersion(RecordBase[VersionedIdentifier, str, RecordEntry]):
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
    def make_key(cls, identifier: VersionedIdentifier,
                 filename: Optional[str] = None) -> str:
        if filename is None:
            return RecordMetadata.make_key(identifier)
        return f'{cls.make_prefix(identifier)}/{filename}'

    @classmethod
    def make_manifest_key(cls, ident: VersionedIdentifier) -> str:
        return f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/{ident.arxiv_id}/{ident}.manifest.json'

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

    @property
    def identifier(self) -> VersionedIdentifier:
        return self.name

    @property
    def metadata(self) -> RecordEntry:
        """JSON document containing canonical e-print metadata."""
        assert 'metadata' in self.members
        return self.members['metadata']

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


class RecordListing(RecordBase[datetime.date, str, RecordEntry]):
    @classmethod
    def make_key(cls, date: datetime.date) -> str:
        return date.strftime(f'{cls.make_prefix(date)}/%Y-%m-%d-listing.json')

    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> str:
        return f'{cls.make_prefix(date)}.manifest.json'

    @classmethod
    def make_prefix(cls, date: datetime.date) -> str:
        return date.strftime(f'announcement/%Y/%m/%d')

    @property
    def listing(self) -> RecordEntry:
        assert 'listing' in self.members
        return self.members['listing']


class RecordListingMonth(RecordBase[YearMonth, datetime.date, RecordListing]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> str:
        """
        Make a key for a monthly listing manifest.

        Returns
        -------
        str

        """
        yr, month = year_and_month
        return f'announcement/{yr}/{yr}-{str(month).zfill(2)}.manifest.json'


class RecordListingYear(RecordBase[Year, YearMonth, RecordListingMonth]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> str:
        """
        Make a key for a yearly listing manifest.

        Returns
        -------
        str

        """
        return f'announcement/{year}.manifest.json'


class RecordListings(RecordBase[str, Year, RecordListingYear]):

    @classmethod
    def make_manifest_key(cls, _: str) -> str:
        """
        Make a key for a root listing manifest.

        Returns
        -------
        str

        """
        return 'announcement.manifest.json'


class RecordEPrint(RecordBase[Identifier, VersionedIdentifier, RecordVersion]):
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


class RecordDay(RecordBase[datetime.date, Identifier, RecordEPrint]):
    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> str:
        """
        Make a key for a daily e-print manifest.

        Returns
        -------
        str

        """
        return date.strftime('e-prints/%Y/%m/%Y-%m-%d.manifest.json')


class RecordMonth(RecordBase[YearMonth, datetime.date, RecordDay]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> str:
        """
        Make a key for a monthly e-print manifest.

        Returns
        -------
        str

        """
        year, month = year_and_month
        return f'e-prints/{year}/{year}-{str(month).zfill(2)}.manifest.json'


class RecordYear(RecordBase[Year, YearMonth, RecordMonth]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> str:
        """
        Make a key for a yearly e-print manifest.

        Returns
        -------
        str

        """
        return f'e-prints/{year}.manifest.json'


class RecordEPrints(RecordBase[str, Year, RecordYear]):
    @classmethod
    def make_manifest_key(cls, _: str) -> str:
        """
        Make a key for all e-print manifest.

        Returns
        -------
        str

        """
        return f'e-prints.manifest.json'


class Record(RecordBase[str,
                        str,
                        Union[RecordEPrints, RecordListings]]):
    @classmethod
    def make_manifest_key(cls, _: str) -> str:
        """
        Make a key for global manifest.

        Returns
        -------
        str

        """
        return f'global.manifest.json'

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