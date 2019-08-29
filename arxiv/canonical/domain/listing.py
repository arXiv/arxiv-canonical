"""Provides domain concepts and logic for the listing."""

import datetime
from typing import NamedTuple, MutableSequence, Mapping, Tuple, Optional

from .eprint import EPrint, Identifier
from .event import Event

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class Listing(NamedTuple):
    """A collection of announcement-related events on a particular day."""

    date: datetime.date
    """Date on which the events occurred."""
    events: MutableSequence[Event]
    """Events in this listing."""

    @property
    def start_datetime(self) -> datetime.datetime:
        if not self.events:
            return datetime.datetime.now()
        return self.events[0].event_date

    @property
    def end_datetime(self) -> datetime.datetime:
        if not self.events:
            return datetime.datetime.now()
        return self.events[-1].event_date


class ListingMonth(NamedTuple):
    """A collection of listings over a month."""

    name: YearMonth
    listings: Mapping[datetime.date, Listing]

    @property
    def year(self) -> Year:
        return self.name[0]

    @property
    def month(self) -> Month:
        return self.name[1]


class ListingYear(NamedTuple):
    """A collection of listings over a year."""

    year: int
    months: Mapping[Tuple[int, int], ListingMonth]


class AllListings(NamedTuple):
    """All listings."""

    name: Optional[str]
    years: Mapping[int, ListingYear]
