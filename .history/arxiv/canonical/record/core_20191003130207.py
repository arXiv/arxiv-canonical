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

from ..serialize.decoder import CanonicalDecoder
from ..serialize.encoder import CanonicalEncoder

from .. import domain as D
from ..util import GenericMonoDict

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class RecordStream(NamedTuple):
    """A single bitstream in the record."""

    domain: D.CanonicalFile

    content: Optional[IO[bytes]]
    """Raw content of the entry."""

    content_type: D.ContentType
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


_EDomain = TypeVar('_EDomain', bound=D.CanonicalBase)
_Self = TypeVar('_Self', bound='RecordEntry')


class RecordEntry(Generic[_EDomain]):
    """
    An entry in the canonical record.

    Comprised of a :class:`.RecordStream` and a domain representation of the
    entry (i.e. the application-level interpretation of the stream).
    """

    key: D.Key
    """Full key (path) at which the entry is stored."""
    domain: _EDomain
    stream: RecordStream

    def __init__(self, key: D.Key, stream: RecordStream, domain: _EDomain) \
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
    def make_manifest_key(cls, name: Name) -> D.Key:  # pylint: disable=unused-argument
        """Generate a full key that can be used to store a manifest."""
        ...  # pylint: disable=pointless-statement ; this is a stub.





class RecordListingDay(RecordBase[datetime.date,
                                  D.ListingIdentifier,
                                  RecordListing,
                                  D.ListingDay]):

    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> D.Key:
        return D.Key(date.strftime('announcement/%Y/%m/%Y-%m-%d.manifest.json'))


class RecordListingMonth(RecordBase[YearMonth,
                                    datetime.date,
                                    RecordListing,
                                    D.ListingMonth]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> D.Key:
        """
        Make a key for a monthly listing manifest.

        Returns
        -------
        str

        """
        yr, month = year_and_month
        return D.Key(f'announcement/{yr}/{yr}-{str(month).zfill(2)}.manifest.json')


class RecordListingYear(RecordBase[Year,
                                   YearMonth,
                                   RecordListingMonth,
                                   D.ListingYear]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> D.Key:
        """
        Make a key for a yearly listing manifest.

        Returns
        -------
        str

        """
        return D.Key(f'announcement/{year}.manifest.json')


class RecordListings(RecordBase[str, Year, RecordListingYear, D.AllListings]):

    @classmethod
    def make_manifest_key(cls, _: str) -> D.Key:
        """
        Make a key for a root listing manifest.

        Returns
        -------
        str

        """
        return D.Key('announcement.manifest.json')


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
        year, month = year_and_month
        return D.Key(f'e-prints/{year}/{year}-{str(month).zfill(2)}.manifest.json')


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


class Record(RecordBase[str,
                        str,
                        Union[RecordEPrints, RecordListings],
                        D.Canon]):
    @classmethod
    def make_manifest_key(cls, _: str) -> D.Key:
        """
        Make a key for global manifest.

        Returns
        -------
        str

        """
        return D.Key(f'global.manifest.json')

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