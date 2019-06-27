"""Provides core domain concepts and logic for the monly listing block."""

from typing import NamedTuple, List, Mapping, Optional
from datetime import date, datetime
from collections import OrderedDict

from .eprint import EPrint
from .event import Event
from .identifier import Identifier, VersionedIdentifier
from .listing import Listing
from .util import now


class MonthlyBlock(NamedTuple):
    """Contains the e-prints announced in a particular calendar month."""

    year: int
    month: int
    eprints: Mapping[VersionedIdentifier, EPrint]

    @property
    def is_open(self) -> bool:
        """Determine whether this block can accept new e-prints."""
        today = date.today()
        return bool(today.year == self.year and today.month == self.month)

    @property
    def is_closed(self) -> bool:
        """Inverse of :attr:`.is_open` (of course)."""
        return not self.is_open

    def get_next_identifier(self) -> Identifier:
        """Get the next available (unused) arXiv identifier in this block."""
        identifiers = sorted(self.eprints.keys(), 
                             key=lambda ident: ident.incremental_part) 
        inc = identifiers[-1].incremental_part + 1 if identifiers else 1
        return Identifier.from_parts(self.year, self.month, inc)

    def add(self, eprint: EPrint) -> None:
        if eprint.versioned_identifier in self.eprints:
            raise ValueError(f'Already exists: {eprint.versioned_identifier}')
        self._check_right_block(eprint)
        self.eprints[eprint.versioned_identifier] = eprint
    
    def update(self, eprint: EPrint) -> None:
        if eprint.versioned_identifier not in self.eprints:
            raise ValueError(f'Not in block: {eprint.versioned_identifier}')
        self._check_right_block(eprint)
        self.eprints[eprint.versioned_identifier] = eprint
    
    def load_eprint(self, arxiv_id: Identifier, 
                    version: Optional[int] = None) -> EPrint:
        if not version:
            version = self._get_latest_version(arxiv_id)
        return self.eprints[VersionedIdentifier.from_parts(arxiv_id, version)]
    
    def _get_latest_version(self, arxiv_id: Identifier) -> int:
        versions = [versioned_identifier for versioned_identifier 
                        in self.eprints.keys() 
                        if versioned_identifier.arxiv_id == arxiv_id]
        if not versions:
            raise KeyError(f'No such eprint: {arxiv_id}')        
        return sorted(versions, key=lambda k: k.version)[-1].version

    def _check_right_block(self, eprint: EPrint) -> None:
        if eprint.arxiv_id.year != self.year \
                or eprint.arxiv_id.month != self.month:
            raise ValueError(f'Wrong block: {eprint.versioned_identifier}')