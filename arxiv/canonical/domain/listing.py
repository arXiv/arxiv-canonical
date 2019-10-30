"""Provides domain concepts and logic for event listings."""

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
    """
    Unique identifier for a :class:`.Listing`.

    Comprised of an ISO-8601 date and a name string.
    """

    def __init__(self, value: str) -> None:
        """Initialize from a raw str value."""
        date_part, self.name = self.split('::', 1)
        self.date = datetime.datetime.strptime(date_part, '%Y-%m-%d').date()

    @classmethod
    def from_parts(cls, date: datetime.date, name: str) -> 'ListingIdentifier':
        """Generate from date and name parts."""
        if ':' in name:
            raise ValueError('Name may not contains colons `:`')
        """Generate a listing identifier from its parts."""
        return cls(date.strftime(f'%Y-%m-%d::{name}'))


class Listing(CanonicalBase):
    """A collection of announcement-related events on a particular day."""

    identifier: ListingIdentifier
    """Unique identifier for this listing, based on the date and name."""

    events: MutableSequence[Event]
    """Events in this listing."""

    def __init__(self, identifier: ListingIdentifier,
                 events: MutableSequence[Event]) -> None:
        """Initialize with a set of events."""
        self.identifier = identifier
        self.events = events

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Listing':
        """Reconstitute from a native dict."""
        return cls(identifier=ListingIdentifier(data['identifier']),
                   events=[Event.from_dict(e)
                           for e in data['events']])

    @property
    def date(self) -> datetime.date:
        """The date of this listing."""
        return self.identifier.date

    @property
    def end_datetime(self) -> datetime.datetime:
        """Timestamp of the most recent event in this listing."""
        if not self.events:
            return datetime.datetime.now()
        return self.events[-1].event_date

    @property
    def number_of_events(self) -> int:
        """Total number of events in this listing."""
        return len(self.events)

    @property
    def number_of_events_by_type(self) -> Dict[EventType, int]:
        """Number of events in this listing by event type."""
        counts: Dict[EventType, int] = defaultdict(int)
        for event in self.events:
            counts[event.event_type] += 1
        return dict(counts.items())

    @property
    def number_of_versions(self) -> int:
        """Total number of :class:`.Version`s represented in this listing."""
        return 0

    @property
    def start_datetime(self) -> datetime.datetime:
        """Timestamp of the earliest event in this listing."""
        if not self.events:
            return datetime.datetime.now()
        return self.events[0].event_date

    def to_dict(self) -> Dict[str, Any]:
        """Generate a native dict representation."""
        return {
            'identifier': str(self.identifier),
            'events': [e.to_dict() for e in self.events]
        }


class ListingDay(CanonicalBase):
    """Represents all of the listings for a particular day."""

    date: datetime.date
    """Date on which the events occurred."""

    listings: Mapping[str, Listing]
    """All of the listings on this date."""

    def __init__(self, date: datetime.date,
                 listings: Mapping[str, Listing]) -> None:
        self.date = date
        self.listings = listings


class ListingMonth(CanonicalBase):
    """A collection of listings over a month."""

    name: YearMonth
    """The year and month of this collection."""

    listings: Mapping[datetime.date, ListingDay]
    """All of the listings in this month."""

    def __init__(self, name: YearMonth,
                 listings: Mapping[datetime.date, ListingDay]) -> None:
        self.name = name
        self.listings = listings

    @property
    def year(self) -> Year:
        """Year represented by this block."""
        return self.name[0]

    @property
    def month(self) -> Month:
        """Month represented by this block."""
        return self.name[1]


class ListingYear(CanonicalBase):
    """A collection of listings over a year."""

    year: int
    """The year of this collection."""

    months: Mapping[Tuple[int, int], ListingMonth]
    """All of the listings in this year."""

    def __init__(self, year: int,
                 months: Mapping[Tuple[int, int], ListingMonth]) -> None:
        self.year = year
        self.months = months


class AllListings(CanonicalBase):
    """All listings in the canonical record."""

    name: Optional[str]

    years: Mapping[int, ListingYear]

    def __init__(self, name: Optional[str],
                 years: Mapping[int, ListingYear]) -> None:
        self.name = name
        self.years = years
