"""
Announcement listings

The announcement listings commemorate the announcement-related events that
occur on a given day. This includes new e-prints/versions, withdrawals,
cross-lists, etc.

The key prefix structure for an announcement listing file is:

```
announcement/<YYYY>/<MM>/<DD>/
```

Each daily key prefix may contain one or more sub-keys. Each sub-key ending in
.json is treated as a listing file. This allows for the possibility of
sharded/multi-threaded announcement processes that write separate listing
files, e.g. for specific classification domains.

``YYYY`` is the year, ``MM`` the month, and ``DD`` the day on which the
announcement events encoded therein occurred and on which the subordinate
listing files were generated.
"""

import io
from json import dumps, load
from typing import NamedTuple, List, IO, Iterator, Tuple
from datetime import datetime, date

from ...domain import CanonicalRecord, Listing
from ..encoder import CanonicalJSONEncoder
from ..decoder import CanonicalJSONDecoder
from .base import BaseEntry, IEntry, checksum, ChecksumError
from .eprint import serialize as serialize_eprint


class ListingEntry(BaseEntry):
    content_type = 'application/json'

    @staticmethod
    def make_key(key_prefix: str) -> str:
        return '/'.join([key_prefix, 'listing.json'])


def make_key_prefix(year: int, month: int, day: int) -> str:
    return '/'.join([
        'announcement', str(year), str(month).zfill(2), str(day).zfill(2)
    ])


def deserialize(record: ListingEntry, validate: bool = True) -> Listing:
    """Deserialize an :class:`.ListingEntry` to an :class:`.Listing`."""
    listing = load(record.content, cls=CanonicalJSONDecoder)
    if validate:    # Compare calculated checksum to recorded checksum.
        if checksum(record.content) != record.checksum:
            raise ChecksumError('Listing has non-matching checksum')
    return listing


def serialize(listing: Listing) -> ListingEntry:
    """Serialize a :class:`.Listing`."""
    listing_json = dumps(listing, cls=CanonicalJSONEncoder)
    listing_content = io.BytesIO(listing_json.encode('utf-8'))
    prefix = make_key_prefix(listing.date.year, listing.date.month,
                             listing.date.day)
    return ListingEntry(key=ListingEntry.make_key(prefix),
                        content=listing_content,
                        checksum=checksum(listing_content))