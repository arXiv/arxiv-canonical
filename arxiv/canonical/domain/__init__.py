"""
Core data structures and concepts used to describe arXiv e-prints.

The structures in this module capture the central ideas and semantics of
e-prints, their versions, and their content. It does not address things like
how the canonical record is encoded in a key-value system, nor mechanisms for
verifying completeness or integrity.
"""

from arxiv.taxonomy import Category

from .base import CanonicalBase, CanonicalBaseCollection
from .block import AllEPrints, EPrintYear, EPrintMonth, EPrintDay
from .content import ContentType, SourceType, available_formats_by_ext, \
    list_source_extensions
from .eprint import EPrint
from .file import CanonicalFile, URI, Key
from .identifier import Identifier, InvalidIdentifier, VersionedIdentifier
from .license import License
from .listing import Listing, ListingDay, ListingMonth, ListingYear, \
    AllListings, ListingIdentifier
from .person import Person
from .version import Version, VersionReference, Metadata, Event, EventType, \
    EventSummary, EventIdentifier

domain_classes = (
    AllEPrints,
    AllListings,
    CanonicalFile,
    Category,
    CanonicalBase,
    CanonicalBaseCollection,
    ContentType,
    EPrint,
    EPrintDay,
    EPrintMonth,
    EPrintYear,
    Event,
    EventIdentifier,
    EventSummary,
    EventType,
    Identifier,
    InvalidIdentifier,
    Key,
    License,
    Listing,
    ListingDay,
    ListingIdentifier,
    ListingYear,
    ListingMonth,
    Metadata,
    Person,
    SourceType,
    URI,
    Version,
    VersionedIdentifier,
    VersionReference,
)


__all__ = [cls.__name__ for cls in domain_classes]
"""All of the core domain classes in this package."""

__all__ += ['available_formats_by_ext', 'list_source_extensions']


class Canon(CanonicalBase):
    """Represents the canonical record as a whole from a domain perspective."""

    def __init__(self, eprints: AllEPrints, listings: AllListings) -> None:
        """The canon is comprised of all e-prints and all listings."""
        self.eprints = eprints
        self.listings = listings