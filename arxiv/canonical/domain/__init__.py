"""Core data structures and concepts."""

from arxiv.taxonomy import Category
from .base import CanonicalBase, CanonicalBaseCollection
from .block import AllEPrints, EPrintYear, EPrintMonth, EPrintDay
from .content import ContentType, SourceType
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


class Canon(CanonicalBase):
    def __init__(self, eprints: AllEPrints, listings: AllListings) -> None:
        self.eprints = eprints
        self.listings = listings