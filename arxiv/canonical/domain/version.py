"""Provides the core domain concept and logic for individual versions."""

import io
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, MutableSequence, \
    NamedTuple, Optional, Tuple, Union
from uuid import UUID

from backports.datetime_fromisoformat import MonkeyPatch
from typing_extensions import Literal

from arxiv.taxonomy import Category  # pylint: disable=no-name-in-module

from .base import CanonicalBase
from .content import ContentType, SourceType
from .identifier import Identifier, VersionedIdentifier
from .person import Person
from .file import CanonicalFile
from .license import License

MonkeyPatch.patch_fromisoformat()


class Metadata(CanonicalBase):
    """Submitter-provided descriptive metadata for a version."""

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
    def from_dict(cls, data: Dict[str, Any]) -> 'Metadata':
        """Reconstitute from a native dict."""
        return cls(
            primary_classification=Category(data['primary_classification']),
            secondary_classification=[
                Category(cat) for cat in data['secondary_classification']
            ],
            title=data['title'],
            abstract=data['abstract'],
            authors=data['authors'],
            license=License.from_dict(data['license']),
            comments=data.get('comments'),
            journal_ref=data.get('journal_ref'),
            report_num=data.get('report_num'),
            doi=data.get('doi'),
            msc_class=data.get('msc_class'),
            acm_class=data.get('acm_class'),
        )

    @property
    def all_categories(self) -> List[str]:
        """All classification categories for this version."""
        return [self.primary_classification] + self.secondary_classification

    def add_secondaries(self, *new_secondaries: Category) -> None:
        """Add cross-list categories for this version."""
        for category in new_secondaries:
            if category not in self.secondary_classification:
                self.secondary_classification.append(category)

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict representation."""
        return {
            'primary_classification': str(self.primary_classification),
            'secondary_classification': [
                str(cat) for cat in self.secondary_classification
            ],
            'title': self.title,
            'abstract': self.abstract,
            'authors': self.authors,
            'license': self.license.to_dict(),
            'comments': self.comments,
            'journal_ref': self.journal_ref,
            'report_num': self.report_num,
            'doi': self.doi,
            'msc_class': self.msc_class,
            'acm_class': self.acm_class
        }


class VersionReference(CanonicalBase):
    """An abridged reference to a particular :class:`Version`."""

    identifier: VersionedIdentifier
    """Identifier of the version."""

    announced_date: date
    """Date on which the version was announced."""

    submitted_date: date
    """Date on which the version was submitted."""

    def __init__(self, identifier: VersionedIdentifier,
                 announced_date: date,
                 submitted_date: date) -> None:
        self.identifier = identifier
        self.announced_date = announced_date
        self.submitted_date = submitted_date

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VersionReference':
        """Reconstitute from a native dict."""
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            announced_date=datetime.fromisoformat(data['announced_date']).date(),  # type: ignore; pylint: disable=no-member
            submitted_date=datetime.fromisoformat(data['submitted_date']).date(),  # type: ignore; pylint: disable=no-member
        )

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict representation."""
        return {
            'identifier': str(self.identifier),
            'announced_date': self.announced_date.isoformat(),
            'submitted_date': self.submitted_date.isoformat(),
        }


class Version(CanonicalBase):
    """Represents a single version of an arXiv e-print in the record."""

    identifier: VersionedIdentifier
    """Unique arXiv identifier for the version."""

    announced_date: date
    """Day on which this version was announced."""

    announced_date_first: date
    """Day on which the first version of the e-print was announced."""

    submitted_date: datetime
    """Timestamp when this version was submitted to arXiv."""

    updated_date: datetime
    """The last time the record for this version was changed."""

    metadata: Metadata
    """Submitter-provided descriptive metadata for the version."""

    events: List['EventSummary']
    """Events that are specific to this version of the e-print."""

    previous_versions: List[VersionReference]
    """References to previous versions of the e-print."""

    submitter: Optional[Person]
    """Person responsible for submitting this version."""

    proxy: Optional[str]
    """The proxy that deposited the version on behalf of the submitter."""

    is_announced: bool
    """Indicate whether or not the version is announced."""

    is_withdrawn: bool
    """Indicate whether or not the version is withdrawn."""

    reason_for_withdrawal: Optional[str]
    """The reason for the withdrawal of the e-print."""

    is_legacy: bool
    """Indicate whether this record was populated from the legacy system."""

    source: CanonicalFile
    """The original user-submitted source package."""

    render: Optional[CanonicalFile]
    """
    Human-readable representation of the e-print.

    Usually a PDF generated from the source, but may also be a user-provided
    PDF.
    """

    source_type: Optional[SourceType]
    """Internal code for the source type."""

    formats: Dict[ContentType, CanonicalFile]
    """Dissemination formats for this version."""

    def __init__(self, identifier: VersionedIdentifier,
                 announced_date: date,
                 announced_date_first: date,
                 submitted_date: datetime,
                 updated_date: datetime,
                 metadata: Metadata,
                 source: CanonicalFile,
                 events: Optional[List['EventSummary']] = None,
                 previous_versions: Optional[List[VersionReference]] = None,
                 submitter: Optional[Person] = None,
                 proxy: Optional[str] = None,
                 is_announced: bool = False,
                 is_withdrawn: bool = False,
                 is_legacy: bool = False,
                 reason_for_withdrawal: Optional[str] = None,
                 source_type: Optional[SourceType] = None,
                 render: Optional[CanonicalFile] = None,
                 formats: Dict[ContentType, CanonicalFile] = {}) -> None:
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
        self.formats = formats

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Version':
        """Reconstitute from a native dict."""
        source_type: Optional[SourceType] = None
        if 'source_type' in data and data['source_type']:
            source_type = SourceType(data['source_type'])

        render: Optional[CanonicalFile] = None
        if 'render' in data and data['render']:
            render = CanonicalFile.from_dict(data['render'])
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            announced_date=datetime.fromisoformat(data['announced_date']).date(),  # type: ignore ; pylint: disable=no-member
            announced_date_first=datetime.fromisoformat(data['announced_date_first']).date(),  # type: ignore ; pylint: disable=no-member
            submitted_date=datetime.fromisoformat(data['submitted_date']),  # type: ignore ; pylint: disable=no-member
            updated_date=datetime.fromisoformat(data['updated_date']),  # type: ignore ; pylint: disable=no-member
            metadata=Metadata.from_dict(data['metadata']),
            events=[EventSummary.from_dict(e) for e in data['events']],
            previous_versions=[VersionReference.from_dict(v) for v in data['previous_versions']],
            submitter=Person.from_dict(data['submitter']) if data.get('submitter') else None,
            proxy=data.get('proxy'),
            is_announced=data['is_announced'],
            is_withdrawn=data['is_withdrawn'],
            reason_for_withdrawal=data.get('reason_for_withdrawal'),
            is_legacy=data['is_legacy'],
            render=render,
            source=CanonicalFile.from_dict(data['source']),
            source_type=source_type,
            formats={
                ContentType(entry["format"]):
                    CanonicalFile.from_dict(entry["content"])
                for entry in data.get('formats', [])
            }
        )

    @property
    def number_of_events(self) -> Literal[0]:
        """Numer of events described by this object (0)."""
        return 0

    @property
    def number_of_versions(self) -> Literal[1]:
        """Number of versions described by this object (1)."""
        return 1

    # TODO: do we still need this? Holdover from classic.
    @property
    def size_kilobytes(self) -> int:
        """Size of the source package in kb."""
        assert self.source is not None
        return int(round(self.source.size_bytes / 1_028))

    def get_format(self, desired_format: str) -> CanonicalFile:
        """Get a particular dissemination format for this version."""
        if desired_format == 'source':
            return self.source
        if desired_format == 'render' and self.render:
            return self.render
        try:
            return self.formats[ContentType(desired_format)]
        except ValueError as e:
            raise ValueError(f'Unknown format: {desired_format}') from e
        except KeyError as e:
            raise KeyError(f'Format {desired_format} not available'
                           f' for {self.identifier}') from e

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict representation."""
        return {
            'identifier': str(self.identifier),
            'announced_date': self.announced_date.isoformat(),
            'announced_date_first': self.announced_date_first.isoformat(),
            'submitted_date': self.submitted_date.isoformat(),
            'updated_date': self.updated_date.isoformat(),
            'metadata': self.metadata.to_dict(),
            'events': [s.to_dict() for s in self.events],
            'previous_versions': [
                v.to_dict() for v in self.previous_versions
            ],
            'submitter': self.submitter.to_dict()
                if self.submitter else None,
            'proxy': self.proxy,
            'is_announced': self.is_announced,
            'is_withdrawn': self.is_withdrawn,
            'reason_for_withdrawal': self.reason_for_withdrawal,
            'is_legacy': self.is_legacy,
            'render': self.render.to_dict() if self.render else None,
            'source': self.source.to_dict(),
            'source_type': str(self.source_type) if self.source_type else None,
            'formats': [
                {
                    "format": fmt.value,
                    "content": cf.to_dict()
                } for fmt, cf in self.formats.items()
            ]
        }


class EventIdentifier(str):
    """Unique identifier for an :class:`.Event`."""

    version_id: VersionedIdentifier
    """Identifier of the :class:`.Version` to which the event pertains."""

    event_date: datetime
    """Timestamp of the event."""

    shard: str
    """Shard ID for the event."""

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
    """Core attributes of an event and its summary."""

    identifier: VersionedIdentifier
    """Identifier of the :class:`.Version` to which the event pertains."""

    event_date: datetime
    """Timestamp of the event."""

    event_type: EventType
    """The type of this event."""

    categories: List[Category]
    """
    Categories related to this event.

    This is an artifact of the format of the legacy daily.log file, and may no
    longer be particularly useful.
    """

    description: str
    """
    Additional information about the event.

    This is currently not used for anything, but could provide a space for
    administrative notes or other information about updates not captured in
    the event ontology and version metadata.
    """

    is_legacy: bool
    """Indicate whether this event was populated from the legacy record."""

    event_agent: Optional[str]  # TODO: do we need this?
    """Agent that generated the event."""

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
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Reconstitute from a native dict."""
        return cls(
            identifier=VersionedIdentifier(data['identifier']),
            event_date=datetime.fromisoformat(data['event_date']),  # type: ignore ; pylint: disable=no-member
            event_type=EventType(data['event_type']),
            categories=[Category(cat) for cat in data['categories']],
            version=Version.from_dict(data['version']),
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
        """The unique identifier for this event."""
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
        """A summary of this event."""
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

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict representation."""
        return {
            'identifier': str(self.identifier),
            'event_date': self.event_date.isoformat(),
            'event_type': self.event_type.value,
            'categories': [str(cat) for cat in self.categories],
            'version': self.version.to_dict(),
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
    """Unique identifier for the event."""

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
    def from_dict(cls, data: Dict[str, Any]) -> 'EventSummary':
        """Reconstitute from a native dict."""
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

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict representation."""
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