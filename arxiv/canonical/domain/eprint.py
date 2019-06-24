"""Provides the core domain concept and logic for e-prints."""

from typing import NamedTuple, Optional, List
from datetime import datetime, date

from .identifier import Identifier
from .event import Event
from .person import Person
from .file import File
from .license import License


class VersionReference(NamedTuple):
    """Reference to an e-print version."""

    arxiv_id: str
    version: int
    submitted_date: datetime
    announced_date: str
    source_type: str
    size_kilobytes: int


class EPrint(NamedTuple):
    """Canonical metadata record for an arXiv e-print."""

    arxiv_id: Optional[Identifier]
    announced_date: Optional[date]

    version: int
    legacy: bool
    submitted_date: datetime
    license: License
    primary_classification: str
    title: str
    abstract: str
    authors: str
    source_type: str    # TODO: make this an enum.
    """Internal code for the source type."""
    size_kilobytes: int
    previous_versions: List[VersionReference]
    secondary_classification: List[str]
    history: List[Event]

    submitter: Optional[Person] = None
    proxy: Optional[str] = None
    comments: Optional[str] = None
    journal_ref: Optional[str] = None
    report_num: Optional[str] = None
    doi: Optional[str] = None
    msc_class: Optional[str] = None
    acm_class: Optional[str] = None

    source_package: Optional[File] = None
    pdf: Optional[File] = None

    @property
    def all_categories(self) -> List[str]:
        return [self.primary_classification.category] \
            + [clsn.category for clsn in self.secondary_classification]

    @property
    def is_announced(self):
        """
        Determine whether or not this e-print has already been announced.

        An e-print is announced when it has been assigned an identifier, and
        the announcement date is set. Replacements or cross-lists that are
        not announced will have identifiers but not announcement dates.
        """
        return self.arxiv_id is not None and self.announced_date is not None

    def announce(self, identifier: Identifier, on: date) -> 'EPrint':
        _, _, *data = self
        return EPrint(identifier, on, *data)