"""Provides the core domain concept and logic for e-prints."""

from typing import NamedTuple, Optional, List
from datetime import datetime, date

from arxiv.taxonomy import Category
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
    version: Optional[int]
    announced_date: Optional[date]

    legacy: bool
    submitted_date: datetime
    license: License
    primary_classification: Category
    title: str
    abstract: str
    authors: str
    source_type: str    # TODO: make this an enum.
    """Internal code for the source type."""
    size_kilobytes: int
    previous_versions: List[VersionReference]
    secondary_classification: List[Category]
    history: List[Event]

    submitter: Optional[Person] = None
    proxy: Optional[str] = None
    comments: Optional[str] = None
    journal_ref: Optional[str] = None
    report_num: Optional[str] = None
    doi: Optional[str] = None
    msc_class: Optional[str] = None
    acm_class: Optional[str] = None

    is_withdrawn: bool = False
    reason_for_withdrawal: Optional[str] = None

    source_package: Optional[File] = None
    pdf: Optional[File] = None

    @property
    def all_categories(self) -> List[str]:
        return [self.primary_classification] + self.secondary_classification

    @property
    def is_announced(self):
        """
        Determine whether or not this e-print has already been announced.

        An e-print is announced when it has been assigned an identifier, and
        the announcement date is set. Replacements or cross-lists that are
        not announced will have identifiers but not announcement dates.
        """
        return self.arxiv_id is not None and self.announced_date is not None

    def as_announced(self, arxiv_id: Identifier, version: int, on: date) \
            -> 'EPrint':
        _, _, _, *data = self
        return EPrint(arxiv_id, version, on, *data)

    def as_withdrawn(self, version: int, on: date) -> 'EPrint':
        return EPrint(
            arxiv_id=self.arxiv_id,
            version=version,
            announced_date=on,
            legacy=self.legacy,
            submitted_date=self.submitted_date,
            license=self.license,
            primary_classification=self.primary_classification,
            title=self.title,
            abstract=self.abstract,
            authors=self.authors,
            source_type=self.source_type,
            size_kilobytes=self.size_kilobytes,
            previous_versions=self.previous_versions,
            secondary_classification=self.secondary_classification,
            history=self.history,
            submitter=self.submitter,
            proxy=self.proxy,
            comments=self.comments,
            journal_ref=self.journal_ref,
            report_num=self.report_num,
            doi=self.doi,
            msc_class=self.msc_class,
            acm_class=self.acm_class,
            is_withdrawn=True,
            reason_for_withdrawal=self.reason_for_withdrawal,
            source_package=self.source_package,
            pdf=self.pdf
        )
    
    def add_secondaries(self, *new_secondaries: Category) -> None:
        for category in new_secondaries:
            if category not in self.secondary_classification:
                self.secondary_classification.append(category)

    @property
    def versioned_identifier(self) -> str:
        if not self.arxiv_id or not self.version:
            raise ValueError('arXiv ID or version not set')
        return f'{self.arxiv_id}v{self.version}'
    