"""Provides domain concepts and logic for the listing."""

import datetime
from collections import defaultdict
from typing import NamedTuple, MutableSequence, Mapping, Tuple, Optional, \
    Any, Dict, Iterable, Callable

from .base import CanonicalBase
from .eprint import EPrint, Identifier
from .version import Event, EventType

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class ListingIdentifier(str):
    def __init__(self, value: str) -> None:
        date_part, self.name = self.split('::', 1)
        self.date = datetime.datetime.strptime(date_part, '%Y-%m-%d').date()

    @classmethod
    def from_parts(cls, date: datetime.date, name: str) -> 'ListingIdentifier':
        if ':' in name:
            raise ValueError('Name may not contains colons `:`')
        """Generate a listing identifier from its parts."""
        return cls(date.strftime(f'%Y-%m-%d::{name}'))


class Listing(CanonicalBase):
    """A collection of announcement-related events on a particular day."""

    identifier: ListingIdentifier
    """Contains the date and name of the listing."""
    events: MutableSequence[Event]
    """Events in this listing."""

    def __init__(self, identifier: ListingIdentifier,
                 events: MutableSequence[Event]) -> None:
        self.identifier = identifier
        self.events = events

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Listing':
        return cls(identifier=ListingIdentifier(data['identifier']),
                   events=[Event.from_dict(e)
                           for e in data['events']])

    @property
    def date(self) -> datetime.date:
        return self.identifier.date

    @property
    def end_datetime(self) -> datetime.datetime:
        if not self.events:
            return datetime.datetime.now()
        return self.events[-1].event_date

    @property
    def number_of_events(self) -> int:
        return len(self.events)

    @property
    def number_of_events_by_type(self) -> Dict[EventType, int]:
        counts: Dict[EventType, int] = defaultdict(int)
        for event in self.events:
            counts[event.event_type] += 1
        return dict(counts.items())

    @property
    def number_of_versions(self) -> int:
        return 0

    @property
    def start_datetime(self) -> datetime.datetime:
        if not self.events:
            return datetime.datetime.now()
        return self.events[0].event_date

    def to_dict(self) -> Dict[str, Any]:
        return {
            'identifier': str(self.identifier),
            'events': [e.to_dict() for e in self.events]
        }


class ListingDay(CanonicalBase):
    date: datetime.date
    """Date on which the events occurred."""
    listings: Mapping[str, Listing]

    def __init__(self, date: datetime.date,
                 listings: Mapping[str, Listing]) -> None:
        self.date = date
        self.listings = listings


class ListingMonth(CanonicalBase):
    """A collection of listings over a month."""

    name: YearMonth
    listings: Mapping[datetime.date, ListingDay]

    def __init__(self, name: YearMonth,
                 listings: Mapping[datetime.date, ListingDay]) -> None:
        self.name = name
        self.listings = listings

    @property
    def year(self) -> Year:
        return self.name[0]

    @property
    def month(self) -> Month:
        return self.name[1]


class ListingYear(CanonicalBase):
    """A collection of listings over a year."""

    year: int
    months: Mapping[Tuple[int, int], ListingMonth]

    def __init__(self, year: int,
                 months: Mapping[Tuple[int, int], ListingMonth]) -> None:
        self.year = year
        self.months = months


class AllListings(CanonicalBase):
    """All listings."""

    name: Optional[str]
    years: Mapping[int, ListingYear]

    def __init__(self, name: Optional[str],
                 years: Mapping[int, ListingYear]) -> None:
        self.name = name
        self.years = years
