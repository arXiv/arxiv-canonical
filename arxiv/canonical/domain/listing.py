"""Provides domain concepts and logic for the listing."""

import datetime
from typing import NamedTuple, MutableSequence, Mapping, Tuple, Sequence

from .eprint import EPrint, Identifier
from .event import Event


class Listing(NamedTuple):
    """A collection of announcement-related events on a particular day."""

    date: datetime.date
    """Date on which the events occurred."""
    events: MutableSequence[Event]
    """Events in this listing."""


class ListingRange(NamedTuple):
    """A collection of listings over a period of time."""

    start_date: datetime.date
    end_date: datetime.date
    listings: Sequence[Listing]


class ListingMonth(NamedTuple):
    """A collection of listings over a month."""

    year: int
    month: int
    listings: Mapping[datetime.date, Listing]


class ListingYear(NamedTuple):
    """A collection of listings over a year."""

    year: int
    months: Mapping[Tuple[int, int], ListingMonth]


class AllListings(NamedTuple):
    """All listings."""

    years: Mapping[int, ListingYear]
