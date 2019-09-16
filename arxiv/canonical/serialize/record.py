"""
Serializers for low-level elements of the canonical record.

Specifically, this maps concepts in :mod:`.domain` to low-level elements in
:mod:`arxiv.canonical.record` and visa-versa.
"""

from io import BytesIO
from json import dumps, load
from typing import Callable, IO, Tuple

from ..domain import Version, ContentType, Listing, CanonicalFile, \
    VersionedIdentifier, URI
from ..record import RecordStream, RecordVersion, RecordMetadata, \
    RecordEntryMembers, RecordListing
from .decoder import CanonicalDecoder
from .encoder import CanonicalEncoder

Key = str
ContentLoader = Callable[[Key], IO[bytes]]










