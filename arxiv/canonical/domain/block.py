"""Provides core domain concepts and logic for the monly listing block."""

from typing import NamedTuple, List, Mapping, Optional
from datetime import date, datetime
from collections import OrderedDict

from .eprint import EPrint
from .event import Event
from .identifier import Identifier
from .listing import Listing
from .util import now


class MonthlyBlock(NamedTuple):
    """Contains the e-prints announced in a particular calendar month."""

    year: int
    month: int
    new: Mapping[Identifier, EPrint] = []
    replaced: Mapping[Identifier, EPrint] = []
    cross_listed: Mapping[Identifier, EPrint] = []

    listings: Mapping[date, 'Listing'] = OrderedDict()

    def __post_init__(self) -> None:
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

    def make_event(self, eprint: EPrint, event_type: Event.Type, 
                   timestamp: Optional[datetime] = None) -> Event:
        if timestamp is None:
            timestamp = now()
        return Event(eprint.arxiv_id, timestamp, event_type, 
                     eprint.all_categories, version=eprint.version)

    def _add(self, eprint_set: Mapping[Identifier, EPrint], eprint: EPrint,
             event_type: Event.Type) -> None:
        eprint_set[eprint.identifier] = eprint
        self.current_listing.add_event(eprint, 
                                       self.make_event(eprint, event_type))

    def add_new(self, eprint: EPrint) -> None:
        self._add(self.new, eprint, Event.Type.New)

    def add_replacement(self, eprint: EPrint) -> None:
        self._add(self.replaced, eprint, Event.Type.REPLACED)

    def add_crosslist(self, eprint: EPrint) -> None:
        self._add(self.cross_listed, eprint, Event.Type.CROSSLIST)

    def add_withdrawal(self, eprint: EPrint) -> None:
        self._add(self.replaced, eprint, Event.Type.WITHDRAWN)
