from typing import NamedTuple, Tuple, Mapping, List, Optional
from datetime import date, datetime

from .eprint import EPrint
from .event import Event
from .block import MonthlyBlock
from .identifier import Identifier
from .util import now

Year = int
Month = int


class CanonicalRecord(NamedTuple):
    """The arXiv canonical record."""

    blocks: Mapping[Tuple[Year, Month], MonthlyBlock]
    """
    Blocks are monthly storage units.

    E-prints are grouped into blocks based on the month in which the first
    version of the e-print was announced.

    This mapping must always produce a result, creating the appropriate
    :class:`.MonthlyBlock` if necessary.
    """

    listings: Mapping[date, 'Listing']
    """Listings are daily streams of e-print events."""

    @property
    def current_block(self) -> MonthlyBlock:
        """Get the current monthly block of announcements."""
        today = date.today()
        return self.blocks[(today.year, today.month)]

    def announce_new(self, eprint: EPrint) -> EPrint:
        """
        Announce a new e-print.

        This involves setting its identifier to the next available identifier,
        version, and announcement date, and updating the appropriate
        :class:`.MonthlyBlock` and :class:`.Listing`.
        """
        if eprint.is_announced:
            raise ValueError(f'E-print already announced: {eprint.arxiv_id}')
        today = date.today()
        version = 1
        eprint = eprint.as_announced(self.current_block.get_next_identifier(),
                                     version, today)
        self.current_block.add(eprint)
        self._emit(eprint, Event.EventType.NEW)
        return eprint

    def announce_replacement(self, eprint: EPrint) -> EPrint:
        """
        Announce a replacement.

        This involves incrementing the version, setting the announcement date,
        and updating the appropriate :class:`.MonthlyBlock` and
        :class:`.Listing`.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.as_announced(eprint.arxiv_id, eprint.version + 1,
                                     date.today())
        self._get_block_for_id(eprint.arxiv_id).add(eprint)
        self._emit(eprint, Event.EventType.REPLACED)
        return eprint

    def announce_withdrawal(self, eprint: EPrint) -> EPrint:
        """
        Announce a withdrawal.

        This involves incrementing the version, setting the withdrawal state
        and announcement date, and updating the appropriate
        :class:`.MonthlyBlock` and :class:`.Listing`.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.as_withdrawn(eprint.version + 1, date.today())
        self._get_block_for_id(eprint.arxiv_id).add(eprint)
        self._emit(eprint, Event.EventType.WITHDRAWAL)
        return eprint

    def announce_crosslist(self, eprint: EPrint) -> EPrint:
        """
        Announce a cross-list.

        This involves updating the e-print and issuing a new event on the
        appropriate :class:`.Listing`.
        """
        self._get_block_for_id(eprint.arxiv_id).update(eprint)
        self._emit(eprint, Event.EventType.CROSSLIST)
        return eprint

    def update(self, eprint: EPrint) -> EPrint:
        """
        Update an e-print.

        This does not generate announcements, nor result in an incremented
        version. Intended for correcting minor errors.
        """
        self._get_block_for_id(eprint.arxiv_id).update(eprint)
        self._emit(eprint, Event.EventType.UPDATED)
        return eprint

    def load_eprint(self, arxiv_id: Identifier,
                    version: Optional[int] = None) -> EPrint:
        block = self._get_block_for_id(arxiv_id)
        return block.load_eprint(arxiv_id, version)

    def _get_block_for_id(self, arxiv_id: Identifier) -> MonthlyBlock:
        return self.blocks[(arxiv_id.year, arxiv_id.month)]

    def _make_event(self, eprint: EPrint, event_type: Event.EventType,
                    timestamp: Optional[datetime] = None) -> Event:
        if timestamp is None:
            timestamp = now()
        return Event(eprint.arxiv_id, timestamp, event_type,
                     eprint.all_categories, version=eprint.version)

    def _emit(self, eprint: EPrint, event_type: Event.EventType) -> None:
        listing = self.listings[date.today()]
        listing.add_event(eprint, self._make_event(eprint, event_type))