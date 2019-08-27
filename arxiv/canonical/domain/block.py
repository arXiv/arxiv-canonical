"""Provides core domain concepts and logic for the monly listing block."""

import collections
import datetime
from typing import NamedTuple, List, Mapping, Optional, Dict, Iterator, Tuple

from typing_extensions import Protocol

from .eprint import EPrint
from .event import Event
from .identifier import Identifier, VersionedIdentifier
from .listing import Listing
from .util import now
from .version import Version


class Day(NamedTuple):
    date: datetime.date
    eprints: Mapping[Identifier, EPrint]

    # @property
    # def is_open(self) -> bool:
    #     """Determine whether this block can accept new e-prints."""
    #     return bool(self.date == datetime.date.today())

    # @property
    # def is_closed(self) -> bool:
    #     """Inverse of :attr:`.is_open` (of course)."""
    #     return not self.is_open

    # def get_next_identifier(self) -> Identifier:
    #     """Get the next available (unused) arXiv identifier in this block."""
    #     identifiers = sorted(self.eprints.keys(),
    #                          key=lambda ident: ident.incremental_part)
    #     inc = identifiers[-1].incremental_part + 1 if identifiers else 1
    #     return Identifier.from_parts(self.date.year, self.date.month, inc)

    # def add(self, version: Version) -> None:
    #     if version.arxiv_id in self.eprints:
    #         raise ValueError(f'Already exists: {eprint.versioned_identifier}')
    #     self._check_right_block(eprint)
    #     self.eprints[version.versioned_identifier] = eprint

    # def update(self, eprint: EPrint) -> None:
    #     if eprint.versioned_identifier not in self.eprints:
    #         raise ValueError(f'Not in block: {eprint.versioned_identifier}')
    #     self._check_right_block(eprint)
    #     self.eprints[eprint.versioned_identifier] = eprint

    # def load_eprint(self, arxiv_id: Identifier,
    #                 version: Optional[int] = None) -> EPrint:
    #     if not version:
    #         version = self._get_latest_version(arxiv_id)
    #     return self.eprints[VersionedIdentifier.from_parts(arxiv_id, version)]

    # def _get_latest_version(self, arxiv_id: Identifier) -> int:
    #     versions = [versioned_identifier for versioned_identifier
    #                 in self.eprints.keys()
    #                 if versioned_identifier.arxiv_id == arxiv_id]
    #     if not versions:
    #         raise KeyError(f'No such eprint: {arxiv_id}')
    #     return sorted(versions, key=lambda k: k.version)[-1].version

    # def _check_right_block(self, eprint: EPrint) -> None:
    #     assert eprint.identifier is not None
    #     if eprint.identifier.year != self.date.year \
    #             or eprint.identifier.month != self.date.month:
    #         raise ValueError(f'Wrong block: {eprint.versioned_identifier}')


class Month(NamedTuple):
    """Contains the e-prints announced in a particular calendar month."""

    year: int
    month: int
    days: Mapping[datetime.date, Day]

    # @property
    # def is_open(self) -> bool:
    #     """Determine whether this block can accept new e-prints."""
    #     today = datetime.date.today()
    #     return bool(today.year == self.year and today.month == self.month)

    # @property
    # def is_closed(self) -> bool:
    #     """Inverse of :attr:`.is_open` (of course)."""
    #     return not self.is_open

    # def get_next_identifier(self) -> Identifier:
    #     """Get the next available (unused) arXiv identifier in this block."""
    #     return self.days[datetime.date.today()].get_next_identifier()

    # def add(self, eprint: EPrint) -> None:
    #     return self.days[datetime.date.today()].add(eprint)

    # def update(self, eprint: EPrint) -> None:
    #     return self.days[eprint.announced_date].update(eprint)

    # def _check_right_block(self, eprint: EPrint) -> None:
    #     assert eprint.identifier is not None
    #     if eprint.identifier.year != self.year \
    #             or eprint.identifier.month != self.month:
    #         raise ValueError(f'Wrong block: {eprint.versioned_identifier}')


class Year(NamedTuple):
    """Contains the e-prints announced in a particular calendar year."""

    year: int
    months: Mapping[Tuple[int, int], Month]


class AllEPrints(NamedTuple):
    years: Mapping[int, Year]