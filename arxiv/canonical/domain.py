"""Core data structures and concepts."""

from typing import NamedTuple, List, Optional, Dict, Mapping, Tuple
from typing_extensions import Protocol
from enum import Enum
from datetime import datetime, date
from collections import OrderedDict

from pytz import UTC

from arxiv import identifier


def now() -> datetime:
    return datetime.now(UTC)


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
    event_date: datetime
    event_type: Type
    categories: List[str]

    description: str = ''
    legacy: bool = False
    event_agent: Optional[str] = None
    version: int = -1


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


class Identifier(str):
    def __init__(self, value: str) -> None:
        super(Identifier, self).__init__(value)
        if identifier.STANDARD.match(value):
            self.is_old_style = False
        elif identifier.OLD_STYLE.match(value):
            self.is_old_style = True
        else:
            raise ValueError('Not a valid arXiv ID')

    @classmethod
    def from_parts(cls, year: int, month: int, inc: int) -> 'Identifier':
        """Generate a new-style identifier from its parts."""
        return cls(f'{year}{month}.{inc.zfill(5)}')

    @property
    def incremental_part(self) -> int:
        """The part of the identifier that is incremental."""
        if self.is_old_style:
            return int(self.split('/', 1)[1][4:])
        return int(self.split('.', 1)[1])

    @property
    def year(self) -> int:
        if self.is_old_style:
            yy = int(self.split('/', 1)[1][0:2])
        else:
            yy = int(self[:2])
        if yy > 90:
            return 1900 + yy
        return 2000 + yy

    @property
    def month(self) -> int:
        if self.is_old_style:
            return int(self.split('/', 1)[1][2:4])
        return int(self[2:4])


class Readable(Protocol):
    def read(self, size: int = -1) -> bytes:
        """
        Read raw bytes content from the resource.

        This should behave more or less like :func:`io.BufferedIOBase.read`.

        Examples might include:

        - A native Python ``file`` object;
        - A closure that, when called, creates a new ``file`` pointer and reads
          it;
        - A closure that, when called, makes an HTTP request and reads the
          resource.

        """
        ...


class File(NamedTuple):
    """Represents a file in the canonical record, e.g. a source package."""

    filename: str
    mime_type: str
    checksum: str
    content: Readable
    created: datetime
    modified: datetime


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
        Determine whether this e-print has already been announced.

        An e-print is announced when it has been assigned an identifier, and
        the announcement date is set. Replacements or cross-lists that are
        not announced will have identifiers but not announcement dates.
        """
        return self.arxiv_id is not None and self.announced_date is not None

    def announce(self, identifier: Identifier, on: date) -> 'EPrint':
        _, _, *data = self
        return EPrint(identifier, on, *data)


class ListingEvent(NamedTuple):
    event: Event
    eprint: EPrint


class Listing(NamedTuple):
    """A collection of announcement-related events."""

    start_date: date
    end_date: date
    events: List[ListingEvent]

    def add_event(self, eprint: EPrint, event: Event) -> None:
        assert eprint.arxiv_id == event.arxiv_id
        assert eprint.version == event.version
        self.events.append(ListingEvent(event, eprint))


class MonthlyBlock(NamedTuple):
    """Contains the e-prints announced in a particular calendar month."""

    year: int
    month: int
    new: List[EPrint] = []
    replaced: List[EPrint] = []
    cross_listed: List[EPrint] = []

    listings: Mapping[date, 'Listing'] = OrderedDict()

    def __post_init__(self) -> None:
        assert all([ep.is_announced for ep in self.eprints])
        self.new.sort(key=lambda ep: ep.identifier.incremental_part)
        self.replaced.sort(key=lambda ep: ep.identifier.incremental_part)
        self.cross_listed.sort(key=lambda ep: ep.identifier.incremental_part)

    @property
    def is_open(self) -> bool:
        return date.today().month == self.month

    @property
    def is_closed(self) -> bool:
        return not self.is_open

    @property
    def current_listing(self):
        today = date.today()
        if today not in self.listings:
            self.listings[today] = Listing(today, today, [])
        return self.listings[today]

    def get_next_identifier(self) -> Identifier:
        """Generate the next available arXiv identifier in this block."""
        last_identifier = self.new[-1].identifier
        return Identifier.from_parts(self.year, self.month,
                                     last_identifier.incremental_part + 1)

    def can_announce(self, eprint: EPrint) -> bool:
        """
        Determine whether this block can announce an :class:`.EPrint`.

        The block must be open, and the e-print must not already be announced.
        """
        return self.is_open and not eprint.is_announced

    def make_event(self, eprint: EPrint, etype: Event.Type) -> Event:
        return Event(eprint.arxiv_id, now(), etype, eprint.all_categories,
                     version=eprint.version)

    def _add(self, eprint_set: List[EPrint], eprint: EPrint,
             etype: Event.Type) -> None:
        eprint_set.append(eprint)
        self.current_listing.add_event(eprint, self.make_event(eprint, etype))

    def add_new(self, eprint: EPrint) -> None:
        self._add_to_listing(self.new, eprint, Event.Type.New)

    def add_replacement(self, eprint: EPrint) -> None:
        self._add_to_listing(self.replaced, eprint, Event.Type.REPLACED)

    def add_crosslist(self, eprint: EPrint) -> None:
        self._add_to_listing(self.cross_listed, eprint, Event.Type.CROSSLIST)

    def add_withdrawal(self, eprint: EPrint) -> None:
        self._add_to_listing(self.replaced, eprint, Event.Type.WITHDRAWN)


class Repository(NamedTuple):

    blocks: Mapping[Tuple[int, int], MonthlyBlock]

    @property
    def current_block(self) -> MonthlyBlock:
        """Get the current monthly block of announcements."""
        td = date.today()
        if (td.year, td.month) not in self.blocks:
            self.blocks[(td.year, td.month)] = MonthlyBlock(td.year, td.month)
        return self.blocks[(td.year, td.month)]

    def announce_new(self, eprint: EPrint) -> EPrint:
        """
        Announce a new e-print.

        This involves setting its identifier to the next available identifier,
        setting the announcement date, and adding an event the appropriate
        :class:`.Listing`.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.announce(self.get_next_identifier(), self.today())
        self.current_block.add_new(eprint)
        return eprint

    def announce_replacement(self, eprint: EPrint) -> EPrint:
        """
        Announce a replacement.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.announce(eprint.arxiv_id, self.today())
        self.current_block.add_replacement(eprint)
        return eprint

    def announce_crosslist(self, eprint: EPrint) -> EPrint:
        """
        Announce a cross-list.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.announce(eprint.arxiv_id, self.today())
        self.current_block.add_replacement(eprint)
        return eprint


domain_classes = [obj for obj in locals().values()
                  if type(obj) is type
                  and tuple in obj.__bases__
                  and hasattr(obj, '_fields')]
