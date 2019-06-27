"""Core data structures and concepts."""

from .license import License
from .person import Person
from .event import Event
from .identifier import Identifier, VersionedIdentifier
from .file import File
from .eprint import EPrint, VersionReference
from .block import MonthlyBlock
from .record import CanonicalRecord
from .listing import Listing


domain_classes = [
    obj for obj in locals().values() 
    if type(obj) is type and tuple in obj.__bases__ and hasattr(obj, '_fields')
]
"""All of the core domain classes in this package."""
