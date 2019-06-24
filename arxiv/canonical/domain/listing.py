"""Provides domain concepts and logic for the listing."""

from typing import NamedTuple, List, Optional
from datetime import date

from .eprint import EPrint, Identifier
from .event import Event


class Listing(NamedTuple):
    """A collection of announcement-related events."""

    start_date: date
    """Inclusive start-date for this listing."""
    end_date: date
    """Inclusive end-date for this listing."""
    events: List[Event]
    """Events in this listing."""

    def add_event(self, eprint: EPrint, event: Event) -> None:
        assert eprint.arxiv_id == event.arxiv_id
        assert eprint.version == event.version
        self.events.append(event)