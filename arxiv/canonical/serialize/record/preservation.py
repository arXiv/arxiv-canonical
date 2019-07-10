"""
Describes the arXiv preservation record.

The preservation record is a daily digest containing e-print content,
announcement listings, and any suppress or remove directives (with
corresponding tombstones).

```
announcement/<listing>.json
e-prints/<arXiv ID>v<version>/
    <arXiv ID>v<version>.json         # Metadata record
    <arXiv ID>v<version>.tar.gz           # Source package
    <arXiv ID>v<version>.pdf              # First PDF
    <arXiv ID>v<version>.manifest.json    # Manifest.
suppress/<arXiv ID>v<version>/tombstone
preservation.manifest.json
```

The ``preservation.manifest.json`` record is similar to the e-print manifest
record; it contains all of the keys and corresponding checksums for the items
in the preservation record.
"""

import io
from json import dumps
from typing import NamedTuple, List, IO, Iterator, Tuple
from datetime import datetime, date

from ...domain import CanonicalRecord, Listing
from ..encoder import CanonicalJSONEncoder
from .base import BaseEntry, BaseDailyEntry, IEntry, checksum
from .eprint import EPrintRecord, BaseEPrintEntry
from .eprint import serialize as serialize_eprint


class PreservationListingEntry(BaseDailyEntry):
    content_type = 'application/json'

    @property
    def key(self) -> str:
        _date = date(year=self.year, month=self.month, day=self.day)
        return f'announcement/listing-{_date.isoformat()}.json'


class SupressionEntry(BaseEPrintEntry):
    content_type = 'text/plain'

    @property
    def key(self) -> str:
        return f'suppress/{self.arxiv_id}v{self.version}/tombstone'


class PreservationManifestEntry(BaseDailyEntry):
    content_type = 'application/json'

    @property
    def key(self) -> str:
        name = '-'.join(['preservation',
                         str(self.year),
                         str(self.month).zfill(2),
                         str(self.day).zfill(2)])
        return f'{name}.manifest.json'


class PreservationRecord(NamedTuple):
    year: int
    month: int
    day: int

    listings: List[PreservationListingEntry]
    eprints: List[EPrintRecord]
    suppress: List[SupressionEntry]
    manifest: PreservationManifestEntry

    def __iter__(self) -> Iterator[Tuple[str, IEntry]]:
        for listing in self.listings:
            yield listing.key, listing
        for eprint in self.eprints:
            for key, entry in eprint:
                yield key, entry
        for suppression in self.suppress:
            yield suppression.key, suppression
        yield self.manifest.key, self.manifest


def serialize(record: CanonicalRecord, for_date: date) -> PreservationRecord:
    """
    Serialize a preservation record for a particular date.

    Parameters
    ----------
    record : :class:`.CanonicalRecord`
        The canonical record from which to draw data.
    for_date : date
        The date for which to generate the preservation record.

    Returns
    -------
    :class:`.PreservationRecord`

    """
    listing = record.listings[for_date]
    listing_entry = _serialize_listing(listing)
    eprints = [
        serialize_eprint(record.load_eprint(arxiv_id, version))
        for arxiv_id, version in
        set((event.arxiv_id, event.version) for event in listing.events)
    ]
    manifest = _serialize_manifest(eprints, listing_entry)
    return PreservationRecord(year=for_date.year,
                              month=for_date.month,
                              day=for_date.day,
                              listings=[listing_entry],
                              eprints=eprints,
                              suppress=[],
                              manifest=manifest)


def _serialize_listing(listing: Listing) -> PreservationListingEntry:
    listing_json = dumps(listing, cls=CanonicalJSONEncoder)
    listing_content = io.BytesIO(listing_json.encode('utf-8'))
    return PreservationListingEntry(year=listing.date.year,
                                    month=listing.date.month,
                                    day=listing.date.day,
                                    content=listing_content,
                                    checksum=checksum(listing_content))


def _serialize_manifest(eprints: List[EPrintRecord],
                        listing: PreservationListingEntry) \
        -> PreservationManifestEntry:
    manifest = {}
    for eprint in eprints:
        manifest.update({
            eprint.get_full_key(eprint.metadata): eprint.metadata.checksum,
            eprint.get_full_key(eprint.source): eprint.source.checksum,
            eprint.get_full_key(eprint.pdf): eprint.pdf.checksum,
            eprint.get_full_key(eprint.manifest): eprint.manifest.checksum
        })
    manifest[listing.key] = listing.checksum

    manifest_content = io.BytesIO(dumps(manifest).encode('utf-8'))
    return PreservationManifestEntry(year=listing.year,
                                     month=listing.month,
                                     day=listing.day,
                                     content=manifest_content,
                                     checksum=checksum(manifest_content))