import datetime
from typing import Optional

from ..domain import Listing
from .base import ICanonicalStorage
from . import methods


def load_listing(storage: ICanonicalStorage, date: datetime.date,
                 checksum: Optional[str] = None) -> Listing:
    return methods.load_listing(storage, date, checksum).domain


def store_listing(storage: ICanonicalStorage, listing: Listing) -> str:
    return methods.store_listing(storage, listing)