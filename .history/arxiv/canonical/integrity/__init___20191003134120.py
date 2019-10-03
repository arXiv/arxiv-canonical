"""
Integrity structs and collections for the canonical record.

This module provides a class hierarchy for integrity and consistency-related
concerns pertaining to the canonical record. The classes herein generate
and validate checksums, and generate manifests.

In order to efficiently verify the completeness and integrity of the record (or
a replica of the record), and to identify the source of inconsistencies,
consistency checks are performed at several levels of granularity (e.g. entry,
day, month, year, global). The completeness and integrity of all or a part of
the arXiv collection can be verified by comparing the checksum values at the
corresponding level of granularity.

The way in which checksum values are calculated for each level is described
below. This is inspired by the strategy for checksum validation of large
chunked uploads to Amazon S3. All checksum values are md5 hashes, stored and
transmitted as URL-safe base64-encoded strings.

+---------+-------------------------+------------------+----------------------+
| Level   | Contents                | Completeness     | Integrity            |
+=========+=========================+==================+======================+
| File    | Binary data.            | Presence/absence | Hash of binary file  |
|         |                         | of descriptor.   | content.             |
+---------+-------------------------+------------------+----------------------+
| Version | Collection of metadata, | Presence         | Hash of concatenated |
|         | source, and render      | of files.        | (sorted by name)     |
|         | files.                  |                  | file hashes.         |
+---------+-------------------------+------------------+----------------------+
| E-Print | One or more sequential  | Presence of      | Hash of concatenated |
|         | versions                | version records. | (sorted) version     |
|         |                         |                  | hashes.              |
+---------+-------------------------+------------------+----------------------+
| Day     | All e-prints the first  | Presence of      | Hash of concatenated |
|         | version of which was    | e-print records. | (sorted) e-print     |
|         | announced on this day.  |                  | hashes.              |
+---------+-------------------------+------------------+----------------------+
| Month   | All e-prints the first  | Presence of day  | Hash of concatenated |
|         | version of which was    | records.         | (sorted) day hashes. |
|         | announced in this       |                  |                      |
|         | month.                  |                  |                      |
+---------+-------------------------+------------------+----------------------+
| Year    | All e-prints the first  | Presence of      | Hash of concatenated |
|         | version of which was    | month records.   | (sorted) month       |
|         | announced in this       |                  | hashes.              |
|         | year.                   |                  |                      |
+---------+-------------------------+------------------+----------------------+
| All     | All e-prints.           | Presence of year | Hash of concatenated |
|         |                         | records.         | (sorted) year        |
|         |                         |                  | hashes.              |
+---------+-------------------------+------------------+----------------------+

The same hierarchy is used for listing files, where the terminal bitstream
is the binary serialized manifest.

A global integrity collection, :class:`.Integrity` draws together the
e-print and listing hierarchies into a final, composite level.
"""

from typing import Union

from ..util import GenericMonoDict

from .core import (IntegrityBase, IntegrityEntry, IntegrityEntryBase,
                   IntegrityEntryMembers, R)
from .listing import (IntegrityListing, IntegrityListingDay,
                      IntegrityListingMonth, IntegrityListingYear,
                      IntegrityListings)
from .metadata import IntegrityMetadata
from .version import (IntegrityVersion, IntegrityEPrint, IntegrityDay,
                      IntegrityMonth, IntegrityYear, IntegrityEPrints)

TopLevelCollection = Union[IntegrityEPrints, IntegrityListings]


__all__ = (
    'IntegrityBase',
    'IntegrityDay',
    'IntegrityEntryBase',
    'IntegrityEntryMembers',
    'IntegrityEPrint',
    'IntegrityEPrints',
    'IntegrityListing',
    'IntegrityListingDay',
    'IntegrityListingMonth',
    'IntegrityListingYear',
    'IntegrityListings',
    'IntegrityMetadata',
    'IntegrityMonth',
    'IntegrityVersion',
    'IntegrityYear',
)


class TopLevelMembers(GenericMonoDict[str, TopLevelCollection]):
    """
    A dict that returns only top level members.

    Consistent with
    ``Mapping[str, Union[IntegrityEPrints, IntegrityListings]]``.
    """

    def __getitem__(self, key: str) -> TopLevelCollection:
        value = dict.__getitem__(self, key)
        assert isinstance(value, (IntegrityEPrints, IntegrityListings))
        return value


class Integrity(IntegrityBase[None,
                              R.Record,
                              str,
                              Union[IntegrityEPrints, IntegrityListings]]):
    """Global integrity collection."""
