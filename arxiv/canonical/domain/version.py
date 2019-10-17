"""
Provides the core domain concept and logic for e-prints.

"""
import io
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, MutableSequence, \
    NamedTuple, Optional, Tuple, Union
from uuid import UUID

from backports.datetime_fromisoformat import MonkeyPatch

from arxiv.taxonomy import Category  # pylint: disable=no-name-in-module

from .base import CanonicalBase, Callback, with_callbacks
from .identifier import Identifier, VersionedIdentifier
from .person import Person
from .file import CanonicalFile
from .license import License

MonkeyPatch.patch_fromisoformat()


class Metadata(CanonicalBase):
    """Metadata for a version."""

    primary_classification: Category
    secondary_classification: List[Category]
    title: str
    abstract: str
    authors: str
    license: License
    comments: Optional[str]
    journal_ref: Optional[str]
    report_num: Optional[str]
    doi: Optional[str]
    msc_class: Optional[str]
    acm_class: Optional[str]

    def __init__(self, primary_classification: Category,
                 secondary_classification: List[Category],
                 title: str,
                 abstract: str,
                 authors: str,
                 license: License,
                 comments: Optional[str] = None,
                 journal_ref: Optional[str] = None,
                 report_num: Optional[str] = None,
                 doi: Optional[str] = None,
                 msc_class: Optional[str] = None,
                 acm_class: Optional[str] = None) -> None:
        self.primary_classification = primary_classification
        self.secondary_classification = secondary_classification
        self.title = title
        self.abstract = abstract
        self.authors = authors
        self.license = license
        self.comments = comments
        self.journal_ref = journal_ref
        self.report_num = report_num
        self.doi = doi
        self.msc_class = msc_class
        self.acm_class = acm_class

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'Metadata':
        return cls(
            primary_classification=Category(data['primary_classification']),
            secondary_classification=[
                Category(cat) for cat in data['secondary_classification']
            ],
            title=data['title'],
            abstract=data['abstract'],
            authors=data['authors'],
            license=License.from_dict(data['license'], callbacks=callbacks),
            comments=data.get('comments'),
            journal_ref=data.get('journal_ref'),
            report_num=data.get('report_num'),
            doi=data.get('doi'),
            msc_class=data.get('msc_class'),
            acm_class=data.get('acm_class'),
        )

    @property
    def all_categories(self) -> List[str]:
        return [self.primary_classification] + self.secondary_classification

    def add_secondaries(self, *new_secondaries: Category) -> None:
        for category in new_secondaries:
            if category not in self.secondary_classification:
                self.secondary_classification.append(category)

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'primary_classification': str(self.primary_classification),
            'secondary_classification': [
                str(cat) for cat in self.secondary_classification
            ],
            'title': self.title,
            'abstract': self.abstract,
            'authors': self.authors,
            'license': self.license.to_dict(callbacks=callbacks),
            'comments': self.comments,
            'journal_ref': self.journal_ref,
            'report_num': self.report_num,
            'doi': self.doi,
            'msc_class': self.msc_class,
            'acm_class': self.acm_class
        }


class VersionReference(CanonicalBase):
    identifier: VersionedIdentifier
    announced_date: date
    submitted_date: date

    def __init__(self, identifier: VersionedIdentifier,
                 announced_date: date,
                 submitted_date: date) -> None:
        self.identifier = identifier
        self.announced_date = announced_date
        self.submitted_date = submitted_date

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'VersionReference':
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            announced_date=datetime.fromisoformat(data['announced_date']).date(),  # type: ignore; pylint: disable=no-member
            submitted_date=datetime.fromisoformat(data['submitted_date']).date(),  # type: ignore; pylint: disable=no-member
        )

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'identifier': str(self.identifier),
            'announced_date': self.announced_date.isoformat(),
            'submitted_date': self.submitted_date.isoformat(),
        }


class Version(CanonicalBase):
    """Canonical record for an arXiv e-print version."""

    identifier: VersionedIdentifier
    announced_date: date
    announced_date_first: date
    submitted_date: datetime
    updated_date: datetime
    """The last time the record for this version was written."""
    metadata: Metadata
    events: List['EventSummary']
    """Events that are specific to this version of the e-print."""
    previous_versions: List[VersionReference]
    submitter: Optional[Person]
    proxy: Optional[str]
    is_announced: bool
    is_withdrawn: bool
    reason_for_withdrawal: Optional[str]
    is_legacy: bool
    render: CanonicalFile
    """
    Human-readable representation of the e-print.

    Usually a PDF generated from the source, but may also be a user-provided
    PDF, HTML document, or other product.
    """
    source: CanonicalFile
    source_type: Optional[str]  # TODO: make this an enum?
    """Internal code for the source type."""

    def __init__(self, identifier: VersionedIdentifier,
                 announced_date: date,
                 announced_date_first: date,
                 submitted_date: datetime,
                 updated_date: datetime,
                 metadata: Metadata,
                 render: CanonicalFile,
                 source: CanonicalFile,
                 events: Optional[List['EventSummary']] = None,
                 previous_versions: Optional[List[VersionReference]] = None,
                 submitter: Optional[Person] = None,
                 proxy: Optional[str] = None,
                 is_announced: bool = False,
                 is_withdrawn: bool = False,
                 is_legacy: bool = False,
                 reason_for_withdrawal: Optional[str] = None,
                 source_type: Optional[str] = None) -> None:
        self.identifier = identifier
        self.announced_date = announced_date
        self.announced_date_first = announced_date_first
        self.submitted_date = submitted_date
        self.updated_date = updated_date
        self.metadata = metadata
        self.events = events or []
        self.previous_versions = previous_versions or []
        self.submitter = submitter
        self.proxy = proxy
        self.is_announced = is_announced
        self.is_withdrawn = is_withdrawn
        self.reason_for_withdrawal = reason_for_withdrawal
        self.is_legacy = is_legacy
        self.render = render
        self.source = source
        self.source_type = source_type

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'Version':
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            announced_date=datetime.fromisoformat(data['announced_date']).date(),  # type: ignore ; pylint: disable=no-member
            announced_date_first=datetime.fromisoformat(data['announced_date_first']).date(),  # type: ignore ; pylint: disable=no-member
            submitted_date=datetime.fromisoformat(data['submitted_date']),  # type: ignore ; pylint: disable=no-member
            updated_date=datetime.fromisoformat(data['updated_date']),  # type: ignore ; pylint: disable=no-member
            metadata=Metadata.from_dict(data['metadata'], callbacks=callbacks),
            events=[EventSummary.from_dict(e, callbacks=callbacks) for e in data['events']],
            previous_versions=[VersionReference.from_dict(v, callbacks=callbacks) for v in data['previous_versions']],
            submitter=Person.from_dict(data['submitter'], callbacks=callbacks) if data.get('submitter') else None,
            proxy=data.get('proxy'),
            is_announced=data['is_announced'],
            is_withdrawn=data['is_withdrawn'],
            reason_for_withdrawal=data.get('reason_for_withdrawal'),
            is_legacy=data['is_legacy'],
            render=CanonicalFile.from_dict(data['render'], callbacks=callbacks),
            source=CanonicalFile.from_dict(data['source'], callbacks=callbacks),
            source_type=data.get('source_type')
        )

    @property
    def number_of_events(self) -> int:
        return 0

    @property
    def number_of_versions(self) -> int:
        return 1

    @property
    def size_kilobytes(self) -> int:
        assert self.source is not None
        return int(round(self.source.size_bytes / 1_028))

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'identifier': str(self.identifier),
            'announced_date': self.announced_date.isoformat(),
            'announced_date_first': self.announced_date_first.isoformat(),
            'submitted_date': self.submitted_date.isoformat(),
            'updated_date': self.updated_date.isoformat(),
            'metadata': self.metadata.to_dict(callbacks=callbacks),
            'events': [s.to_dict(callbacks=callbacks) for s in self.events],
            'previous_versions': [
                v.to_dict(callbacks=callbacks) for v in self.previous_versions
            ],
            'submitter': self.submitter.to_dict(callbacks=callbacks)
                if self.submitter else None,
            'proxy': self.proxy,
            'is_announced': self.is_announced,
            'is_withdrawn': self.is_withdrawn,
            'reason_for_withdrawal': self.reason_for_withdrawal,
            'is_legacy': self.is_legacy,
            'render': self.render.to_dict(callbacks=callbacks),
            'source': self.source.to_dict(callbacks=callbacks),
            'source_type': self.source_type
        }


class EventIdentifier(str):
    version_id: VersionedIdentifier
    event_date: datetime
    shard: str

    def __init__(self, value: str) -> None:
        decoded = urlsafe_b64decode(value).decode('utf-8')
        version_id_raw, event_date_raw, self.shard = decoded.split('::', 2)
        self.version_id = VersionedIdentifier(version_id_raw)
        self.event_date = datetime.fromisoformat(event_date_raw)  # type: ignore ; pylint: disable=no-member

    @classmethod
    def from_parts(cls, identifier: VersionedIdentifier, event_date: datetime,
                   shard: str) -> 'EventIdentifier':
        """Generate a event identifier from its parts."""
        raw = f'{identifier}::{event_date}::{shard}'.encode('utf-8')
        return cls(urlsafe_b64encode(raw).decode('utf-8'))


class EventType(Enum):
    """Supported event types."""

    NEW = 'new'
    UPDATED = 'update'
    UPDATED_METADATA = 'update_metadata'
    REPLACED = 'replace'
    CROSSLIST = 'cross'
    JREF = 'jref'    # Deprecated.
    WITHDRAWN = 'withdraw'
    MIGRATE = 'migrate'
    MIGRATE_METADATA = 'migrate_metadata'

    @property
    def is_new_version(self) -> bool:
        """Indicate whether or not this event type results in a new version."""
        return self in [self.NEW, self.REPLACED, self.WITHDRAWN]


class _EventBase(CanonicalBase):
    identifier: VersionedIdentifier
    event_date: datetime
    event_type: EventType
    categories: List[Category]

    description: str
    is_legacy: bool
    event_agent: Optional[str]

    def __init__(self, identifier: VersionedIdentifier,
                 event_date: datetime,
                 event_type: EventType,
                 categories: Optional[List[Category]] = None,
                 description: str = '',
                 is_legacy: bool = False,
                 event_agent: Optional[str] = None) -> None:
        self.identifier = identifier
        self.event_date = event_date
        self.event_type = event_type
        if categories is None:
            categories = []
        self.categories = categories
        self.description = description
        self.is_legacy = is_legacy
        self.event_agent = event_agent


class Event(_EventBase):
    """An announcement-related event."""

    version: Version
    """The current state of the version (i.e. after the event)."""

    def __init__(self, identifier: VersionedIdentifier,
                 event_date: datetime,
                 event_type: EventType,
                 version: Version,
                 categories: Optional[List[Category]] = None,
                 description: str = '',
                 is_legacy: bool = False,
                 event_agent: Optional[str] = None) -> None:

        self.version = version
        super(Event, self).__init__(identifier, event_date, event_type,
                                    categories=categories,
                                    description=description,
                                    is_legacy=is_legacy,
                                    event_agent=event_agent)

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'Event':
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            event_date=datetime.fromisoformat(data['event_date']),  # type: ignore ; pylint: disable=no-member
            event_type=EventType(data['event_type']),
            categories=[Category(cat) for cat in data['categories']],
            version=Version.from_dict(data['version'], callbacks=callbacks),
            description=data['description'],
            is_legacy=data['is_legacy'],
            event_agent=data.get('event_agent')
        )

    @classmethod
    def get_default_shard(cls) -> str:
        """Get the default listing shard for this event."""
        return 'listing'

    @property
    def event_id(self) -> EventIdentifier:
        return EventIdentifier.from_parts(self.identifier, self.event_date,
                                          self.shard)

    # 2019-09-02: There is not currently a driver for sharding listings, but it
    # is easier to add support for it now then to retrofit later (YAGNI be
    # darned). We can readily imagine, for example, wanting to shard by event
    # type or by primary category. If there is more than one possible return
    # value for this function (as a function of the event data), then multiple
    # listing files will be created accordingly.
    @property
    def shard(self) -> str:
        """The shard name for this event."""
        return self.get_default_shard()

    @property
    def summary(self) -> 'EventSummary':
        return EventSummary(
            identifier=self.identifier,
            event_date=self.event_date,
            event_type=self.event_type,
            event_id=self.event_id,
            categories=self.categories,
            description=self.description,
            is_legacy=self.is_legacy,
            event_agent=self.event_agent
        )

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'identifier': str(self.identifier),
            'event_date': self.event_date.isoformat(),
            'event_type': self.event_type.value,
            'categories': [str(cat) for cat in self.categories],
            'version': self.version.to_dict(callbacks=callbacks),
            'description': self.description,
            'is_legacy': self.is_legacy,
            'event_agent': self.event_agent,
            'event_id': self.event_id
        }


class EventSummary(_EventBase):
    """
    A lightweight description of an event.

    This has all of the data of the original :class:`.Event` except for the
    state of the e-print version.
    """

    event_id: EventIdentifier

    def __init__(self, identifier: VersionedIdentifier,
                 event_date: datetime,
                 event_type: EventType,
                 event_id: EventIdentifier,
                 categories: Optional[List[Category]] = None,
                 description: str = '',
                 is_legacy: bool = False,
                 event_agent: Optional[str] = None) -> None:
        self.event_id = event_id
        super(EventSummary, self).__init__(identifier, event_date, event_type,
                                           categories=categories,
                                           description=description,
                                           is_legacy=is_legacy,
                                           event_agent=event_agent)

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'EventSummary':
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            event_date=datetime.fromisoformat(data['event_date']),  # type: ignore ; pylint: disable=no-member
            event_type=EventType(data['event_type']),
            event_id=EventIdentifier(data['event_id']),
            categories=[Category(cat) for cat in data['categories']],
            description=data['description'],
            is_legacy=data['is_legacy'],
            event_agent=data.get('event_agent')
        )

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'identifier': str(self.identifier),
            'event_date': self.event_date.isoformat(),
            'event_type': self.event_type.value,
            'categories': [str(cat) for cat in self.categories],
            'description': self.description,
            'is_legacy': self.is_legacy,
            'event_agent': self.event_agent,
            'event_id': self.event_id
        }