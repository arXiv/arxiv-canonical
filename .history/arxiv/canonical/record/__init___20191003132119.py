"""
Defines how the canonical record is represented in a key-binary system.

The bulk of this module is concerned with how keys for records and record
manifests are generated.

Classes in this module are largely isomorphic to those in :mod:`.domain`.
:class:`.RecordEntry` represents content at the bitstream level, e.g. a file
containing a listings document or a render PDF. Collections of entries are
based on :class:`RecordBase`, and are composed hierarchically with the apex
at :class:`.Record`.
"""

from typing import Union
from .core import RecordBase, RecordEntry, D
from .file import RecordFile
from .listing import (RecordListing, RecordListingDay, RecordListingMonth,
                      RecordListingYear, RecordListings)
from .metadata import RecordMetadata
from .version import (RecordVersion, RecordEPrint, RecordDay, RecordMonth,
                      RecordYear, RecordEPrints)


class Record(RecordBase[str,
                        str,
                        Union[RecordEPrints, RecordListings],
                        D.Canon]):
    """The apex container for the canonical record."""

    @classmethod
    def make_manifest_key(cls, _: str) -> D.Key:
        """
        Make a key for global manifest.

        Returns
        -------
        str

        """
        return D.Key(f'global.manifest.json')

    @property
    def eprints(self) -> RecordEPrints:
        assert 'eprints' in self.members
        assert isinstance(self.members['eprints'], RecordEPrints)
        return self.members['eprints']

    @property
    def listings(self) -> RecordListings:
        assert 'listings' in self.members
        assert isinstance(self.members['listings'], RecordListings)
        return self.members['listings']