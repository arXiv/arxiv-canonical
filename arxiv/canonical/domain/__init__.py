"""Core data structures and concepts."""

from arxiv.taxonomy import Category
from .block import AllEPrints, Year, Month, Day
from .content import ContentType
from .eprint import EPrint
from .event import Event, EventType
from .file import File
from .identifier import Identifier, VersionedIdentifier
from .license import License
from .listing import Listing, ListingMonth, ListingYear, AllListings
from .person import Person
from .record import CanonicalRecord
from .version import Version, VersionReference


domain_classes = [
    obj for obj in locals().values()
    if type(obj) is type and tuple in obj.__bases__ and hasattr(obj, '_fields')
]
"""All of the core domain classes in this package."""
