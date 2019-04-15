"""Core data structures."""

from typing import NamedTuple, List, Optional
from enum import Enum
from datetime import datetime


class ClassificationTerm(NamedTuple):
    """Classification term from the arXiv taxonomy."""

    id: str
    name: str


class Classification(NamedTuple):
    """An arXiv e-print classification."""

    group: ClassificationTerm
    archive: ClassificationTerm
    category: ClassificationTerm


class License(NamedTuple):
    """License under which the e-print was provided to arXiv."""

    href: str
    label: str


class Person(NamedTuple):
    """An arXiv user."""

    full_name: str
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    suffix: Optional[str] = None
    orcid: Optional[str] = None
    author_id: Optional[str] = None
    affiliation: List[str] = []


class Event(NamedTuple):
    """An announcement-related event."""

    class Type(Enum):
        """Supported event types."""

        CREATED = 'created'
        UPDATED = 'updated'
        CROSSLIST = 'cross'
        WITHDRAWN = 'withdrawn'

    arxiv_id: str
    version: int
    event_date: datetime
    event_type: Type
    event_agent: str
    description: str
    legacy: bool = False


class VersionReference(NamedTuple):
    """Reference to an e-print version."""

    arxiv_id: str
    version: int
    submitted_date: datetime
    announced_date: str


class EPrintMetadata(NamedTuple):
    """Canonical metadata record for an arXiv e-print."""

    arxiv_id: str
    version: int
    legacy: bool
    submitter: Person
    submitted_date: datetime
    announced_date: str
    license: License
    primary_classification: Classification
    title: str
    abstract: str
    authors: str
    comments: str
    journal_ref: str
    report_num: str
    doi: str

    previous_versions: List[VersionReference] = []
    history: List[Event] = []
    secondary_classification: List[Classification] = []
    msc_class: List[str] = []
    acm_class: List[str] = []


domain_classes = [obj for obj in locals().values()
                  if type(obj) is type
                  and tuple in obj.__bases__
                  and hasattr(obj, '_fields')]
