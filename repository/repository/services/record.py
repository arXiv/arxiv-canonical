"""Service integration module for reading the canonical record."""

from arxiv.canonical.domain import EPrint, CanonicalRecord, Identifier, \
    VersionedIdentifier
from arxiv.canonical.services.store import FakeCanonicalStore, DoesNotExist


# TODO: switch to the real store.CanonicalStore implementation when it is 
# available.
class CanonicalStore(FakeCanonicalStore):
    """
    Extension of :class:`.store.CanonicalStore` with additional read patterns.
    """

    def load_eprint(self, identifier: Identifier, version: int) -> EPrint:
        """Load an :class:`.EPrint` from the record."""
        record = self.load_record()
        v_identifier = VersionedIdentifier.from_parts(identifier, version)
        return record.load_eprint(v_identifier)