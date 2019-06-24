"""Core data structures and concepts."""

from .license import License
from .person import Person
from .event import Event
from .classification import Classification
from .identifier import Identifier
from .file import File
from .eprint import EPrint, VersionReference
from .block import MonthlyBlock
from .repository import Repository
from .listing import Listing


domain_classes = [
    obj for obj in locals().values() 
    if type(obj) is type and tuple in obj.__bases__ and hasattr(obj, '_fields')
]
"""All of the core domain classes in this package."""
