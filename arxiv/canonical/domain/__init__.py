"""Core data structures and concepts."""

from arxiv.taxonomy import Category
from .block import AllEPrints, EPrintYear, EPrintMonth, EPrintDay
from .content import ContentType
from .eprint import EPrint
from .event import Event, EventType
from .file import CanonicalFile
from .identifier import Identifier, VersionedIdentifier
from .license import License
from .listing import Listing, ListingMonth, ListingYear, AllListings
from .person import Person
from .record import CanonicalRecord
from .version import Version, VersionReference, Metadata


domain_classes = [
    obj for obj in locals().values()
    if type(obj) is type and tuple in obj.__bases__ and hasattr(obj, '_fields')
]
"""All of the core domain classes in this package."""


class Canon:
    def __init__(self, eprints: AllEPrints, listings: AllListings) -> None:
        self.eprints = eprints
        self.listings = listings