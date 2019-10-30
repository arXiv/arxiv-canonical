"""Base classes and core concepts for :mod:`arxiv.canonical.record`."""

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
    def from_domain(cls: Type[_Self], d: _EDomain) -> _Self:
        raise NotImplementedError("Must be implemented by child class")

    @classmethod
    def to_domain(cls, stream: RecordStream) -> _EDomain:
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

