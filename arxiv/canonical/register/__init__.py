"""
Register for the canonical record.

This module implements the high-level API for the arXiv canonical record. It
orchestrates the classes in :mod:`arxiv.canonical.domain`,
:mod:`arxiv.canonical.record`, and :mod:`arxiv.canonical.integrity` to
implement reading from and writing to the record.
"""

from .api import (RegisterAPI, IRegisterAPI, ICanonicalStorage,
                  ICanonicalSource, IStorableEntry,
                  Base, RegisterDay, RegisterEPrint,
                  RegisterEPrints, RegisterListing, RegisterListings,
                  RegisterListingDay, RegisterListingMonth,
                  RegisterListingYear, RegisterMetadata, RegisterMonth,
                  RegisterVersion, RegisterYear, NoSuchResource,
                  ConsistencyError)

__all__ = (
    'Base',
    'ConsistencyError',
    'IRegisterAPI',
    'ICanonicalStorage',
    'ICanonicalSource',
    'IStorableEntry',
    'NoSuchResource',
    'RegisterAPI',
    'RegisterDay',
    'RegisterEPrint',
    'RegisterEPrints',
    'RegisterListing',
    'RegisterListings',
    'RegisterListingDay',
    'RegisterListingMonth',
    'RegisterListingYear',
    'RegisterMetadata',
    'RegisterMonth',
    'RegisterVersion',
    'RegisterYear'
)