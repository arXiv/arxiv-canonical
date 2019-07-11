"""
Describes the arXiv preservation record.


"""

import io
from json import dumps
from typing import NamedTuple, List, IO, Iterator, Tuple
from datetime import datetime, date

from ...domain import CanonicalRecord, Listing
from ..encoder import CanonicalJSONEncoder
from .base import BaseEntry, IEntry, checksum, ChecksumError
from .eprint import EPrintRecord
from .eprint import serialize as serialize_eprint
from .listing import ListingEntry


class SupressionEntry(BaseEntry):
    content_type = 'text/plain'

    @staticmethod
    def make_key(key_prefix: str, arxiv_id: str, version: int) -> str:
        return '/'.join([
            key_prefix, 'suppress', f'{arxiv_id}v{version}', 'tombstone'
        ])


class PreservationManifestEntry(BaseEntry):
    """
    Preservation manifest.

    This record is similar to the :class:`.ManifestEntry`; it contains all of
    the keys and corresponding checksums for the items in the preservation
    record.
    """
    content_type = 'application/json'

    @staticmethod
    def make_key(key_prefix: str, year: int, month: int, day: int) -> str:
        name = '-'.join(['preservation',
                         str(year),
                         str(month).zfill(2),
                         str(day).zfill(2)])
        return '/'.join([key_prefix, f'{name}.manifest.json'])


class PreservationRecord(NamedTuple):
    """
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

    """
    year: int
    month: int
    day: int

    listings: List[ListingEntry]
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

    @staticmethod
    def key_prefix(year: int, month: int, day: int) -> str:
        return '/'.join([
            'archive', str(year), str(month).zfill(2), str(day).zfill(2)
        ])


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
    prefix = PreservationRecord.key_prefix(listing.date.year,
                                           listing.date.month,
                                           listing.date.day)

    listing_entry = _serialize_listing(listing, prefix)
    eprints = [
        serialize_eprint(record.load_eprint(arxiv_id, version),
                         prefix='/'.join([prefix, 'e-prints']))
        for arxiv_id, version in
        set((event.arxiv_id, event.version) for event in listing.events)
    ]
    manifest = _serialize_manifest(eprints, listing_entry, listing, prefix)
    return PreservationRecord(year=for_date.year,
                              month=for_date.month,
                              day=for_date.day,
                              listings=[listing_entry],
                              eprints=eprints,
                              suppress=[],
                              manifest=manifest)



def _serialize_listing(listing: Listing, prefix: str) -> ListingEntry:
    listing_json = dumps(listing, cls=CanonicalJSONEncoder)
    listing_content = io.BytesIO(listing_json.encode('utf-8'))
    listing_prefix = '/'.join([prefix, 'announcement'])
    return ListingEntry(key=ListingEntry.make_key(listing_prefix),
                        content=listing_content)


def _serialize_manifest(eprints: List[EPrintRecord],
                        listing_entry: ListingEntry, listing: Listing,
                        prefix: str) -> PreservationManifestEntry:
    manifest = {}
    for eprint in eprints:
        manifest.update({eprint.metadata.key: eprint.metadata.checksum,
                         eprint.source.key: eprint.source.checksum,
                         eprint.pdf.key: eprint.pdf.checksum,
                         eprint.manifest.key: eprint.manifest.checksum})
    manifest[listing_entry.key] = listing_entry.checksum

    manifest_content = io.BytesIO(dumps(manifest).encode('utf-8'))
    key = PreservationManifestEntry.make_key(prefix,
                                             listing.date.year,
                                             listing.date.month,
                                             listing.date.day)
    return PreservationManifestEntry(key=key, content=manifest_content)