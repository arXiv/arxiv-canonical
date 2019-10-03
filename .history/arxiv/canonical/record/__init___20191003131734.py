from typing import Union
from .core import RecordBase, D
from .listing import RecordListings
from .version import RecordEPrints


class Record(RecordBase[str,
                        str,
                        Union[RecordEPrints, RecordListings],
                        D.Canon]):
    """The top-level container for the canonical record."""

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