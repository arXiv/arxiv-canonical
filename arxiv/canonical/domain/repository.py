from typing import NamedTuple, Tuple, Mapping, List
from datetime import date

from .eprint import EPrint
from .block import MonthlyBlock


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
        eprint = eprint.announce(self.current_block.get_next_identifier(), 
                                 date.today())
        self.current_block.add_new(eprint)
        return eprint

    def announce_replacement(self, eprint: EPrint) -> EPrint:
        """
        Announce a replacement.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.announce(eprint.arxiv_id, date.today())
        self.current_block.add_replacement(eprint)
        return eprint

    def announce_crosslist(self, eprint: EPrint) -> EPrint:
        """
        Announce a cross-list.
        """
        if not self.current_block.can_announce(eprint):
            raise ValueError('Cannot announce this e-print')
        eprint = eprint.announce(eprint.arxiv_id, date.today())
        self.current_block.add_replacement(eprint)
        return eprint