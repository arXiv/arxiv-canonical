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
from json import dumps
from typing import NamedTuple, List, IO, Iterator, Tuple
from datetime import datetime, date

from ...domain import CanonicalRecord, Listing
from ..encoder import CanonicalJSONEncoder
from .base import EPrintRecord, BaseEntry, BaseEPrintEntry, BaseDailyEntry, \
    IEntry, checksum
from .base import serialize as serialize_eprint


class ListingEntry(BaseDailyEntry):
    content_type = 'application/json'

    @property
    def key(self) -> str:
        _date = date(year=self.year, month=self.month, day=self.day)
        return '/'.join(['announcement',
                         str(self.year),
                         str(self.month).zfill(2),
                         str(self.day).zfill(2),
                         'listing.json'])


def serialize(listing: Listing) -> ListingEntry:
    """Serialize a :class:`.Listing`."""
    listing_json = dumps(listing, cls=CanonicalJSONEncoder)
    listing_content = io.BytesIO(listing_json.encode('utf-8'))
    return ListingEntry(year=listing.date.year,
                        month=listing.date.month,
                        day=listing.date.day,
                        content=listing_content,
                        checksum=checksum(listing_content))