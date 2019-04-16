"""Core data structures."""

from typing import NamedTuple, List, Optional
from enum import Enum
from datetime import datetime


class License(NamedTuple):
    """License under which the e-print was provided to arXiv."""

    href: str


class Person(NamedTuple):
    """An arXiv user."""

    full_name: str
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    suffix: Optional[str] = None
    orcid: Optional[str] = None
    author_id: Optional[str] = None
    affiliation: Optional[List[str]] = None


class Event(NamedTuple):
    """An announcement-related event."""

    class Type(Enum):
        """Supported event types."""

        NEW = 'new'
        UPDATED = 'updated'
        REPLACED = 'replaced'
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
    source_type: str
    size_kilobytes: int


class Classification(NamedTuple):
    archive: str
    category: Optional[str] = None


class EPrintMetadata(NamedTuple):
    """Canonical metadata record for an arXiv e-print."""

    arxiv_id: str
    version: int
    legacy: bool
    submitted_date: datetime
    announced_date: str
    license: License
    primary_classification: str
    title: str
    abstract: str
    authors: str
    source_type: str
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


class ListingEvent(NamedTuple):
    event: Event
    eprint: EPrintMetadata


class Listing(NamedTuple):
    """A collection of announcement-related events."""

    start_date: datetime
    end_date: datetime
    events: List[ListingEvent]


domain_classes = [obj for obj in locals().values()
                  if type(obj) is type
                  and tuple in obj.__bases__
                  and hasattr(obj, '_fields')]
