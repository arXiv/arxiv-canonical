"""
Provides the core domain concept and logic for e-prints.

"""
import io
from datetime import datetime, date
from typing import NamedTuple, Optional, List

from arxiv.taxonomy import Category

from .identifier import Identifier, VersionedIdentifier
from .event import Event
from .person import Person
from .file import File
from .license import License


class Metadata(NamedTuple):
    """Metadata for a version."""
    primary_classification: Category
    secondary_classification: List[Category]

    title: str
    abstract: str
    authors: str

    license: License

    comments: Optional[str] = None
    journal_ref: Optional[str] = None
    report_num: Optional[str] = None
    doi: Optional[str] = None
    msc_class: Optional[str] = None
    acm_class: Optional[str] = None

    @property
    def all_categories(self) -> List[str]:
        return [self.primary_classification] + self.secondary_classification

    def add_secondaries(self, *new_secondaries: Category) -> None:
        for category in new_secondaries:
            if category not in self.secondary_classification:
                self.secondary_classification.append(category)


class VersionReference(NamedTuple):
    identifier: VersionedIdentifier
    announced_date: date
    submitted_date: date


class Version(NamedTuple):
    """Canonical record for an arXiv e-print version."""

    identifier: Optional[Identifier]
    version: Optional[int]
    announced_date: Optional[date]
    announced_date_first: Optional[date]
    submitted_date: datetime
    metadata: Metadata

    submitter: Optional[Person] = None
    proxy: Optional[str] = None

    is_announced: bool = False
    is_withdrawn: bool = False
    reason_for_withdrawal: Optional[str] = None
    is_legacy: bool = False

    render: Optional[File] = None
    """
    Human-readable representation of the e-print.

    Usually a PDF generated from the source, but may also be a user-provided
    PDF, HTML document, or other product.
    """
    source: Optional[File] = None

    source_type: Optional[str] = None    # TODO: make this an enum?
    """Internal code for the source type."""

    previous_versions: List[VersionReference] = []

    def with_files(self, source: Optional[File] = None,
                   render: Optional[File] = None) -> 'Version':
        return self._replace(source=source, render=render)

    def as_announced(self) -> 'Version':
        return self._replace(is_announced=True)

    def as_withdrawn(self, version: int, on: date) -> 'Version':
        return self._replace(is_withdrawn=True, version=version,
                             announced_date=on)

    @property
    def versioned_identifier(self) -> VersionedIdentifier:
        if not self.arxiv_id or not self.version:
            raise ValueError('arXiv ID or version not set')
        return VersionedIdentifier(f'{self.arxiv_id}v{self.version}')

    @property
    def size_kilobytes(self) -> int:
        assert self.source is not None
        return self.source.size_kilobytes


