"""Provides :class:`.EPrint`."""

from datetime import date
from typing import Optional, NamedTuple, Mapping

from .base import CanonicalBase
from .identifier import Identifier, VersionedIdentifier
from .version import Version


class EPrint(CanonicalBase):
    """
    Core concept of an e-print in the canonical record.

    An e-print is a collection of one or more sequential :class:`.Version`s,
    generally representing projections of a single scholarly work over time.

    E-prints are identified by :class:`.Identifier`s.
    """

    def __init__(self, identifier: Optional[Identifier],
                 versions: Mapping[VersionedIdentifier, Version]) -> None:
        """Initialize with an identifier and a set of versions."""
        self.identifier = identifier
        self.versions = versions

    @property
    def announced_date(self) -> Optional[date]:
        """Date on which the first version of this e-print was announced."""
        idents = [v for v in self.versions]
        return self.versions[idents[0]].announced_date

    @property
    def is_withdrawn(self) -> bool:
        """Indicate whether this e-print has been withdrawn."""
        idents = [v for v in self.versions]
        return self.versions[idents[-1]].is_withdrawn

    # TODO: this is a legacy hold-over; reconsider whether we need it for
    # anything.
    @property
    def size_kilobytes(self) -> int:
        """Indicate the size of the current version of this e-print in kb."""
        idents = [v for v in self.versions]
        return self.versions[idents[-1]].size_kilobytes