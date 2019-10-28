"""Structures for organizing e-prints into periods of time."""

import collections
import datetime
from typing import NamedTuple, List, Mapping, Optional, Dict, Iterator, Tuple

from typing_extensions import Protocol

from .base import CanonicalBase
from .eprint import EPrint
from .version import Event
from .identifier import Identifier, VersionedIdentifier
from .listing import Listing
from .util import now
from .version import Version

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class EPrintDay(CanonicalBase):
    """E-prints originally announced on a specific day."""

    def __init__(self, date: datetime.date,
                 eprints: Mapping[Identifier, EPrint]) -> None:
        """Initialize with e-prints for a particular day."""
        self.date = date
        self.eprints = eprints


class EPrintMonth(CanonicalBase):
    """E-prints originally announced in a particular calendar month."""

    def __init__(self, name: YearMonth,
                 days: Mapping[datetime.date, EPrintDay]) -> None:
        """Initialize with e-prints for a particular month."""
        self.name = name
        self.days = days

    @property
    def year(self) -> Year:
        return self.name[0]

    @property
    def month(self) -> Month:
        return self.name[1]


class EPrintYear(CanonicalBase):
    """E-prints originally announced in a particular calendar year."""

    def __init__(self, year: Year,
                 months: Mapping[Tuple[int, int], EPrintMonth]) -> None:
        """Initialize with e-prints for a particular year."""
        self.year = year
        self.months = months


class AllEPrints(CanonicalBase):
    """Represents the complete set of announced e-prints."""

    def __init__(self, name: str,
                 years: Mapping[int, EPrintYear]) -> None:
        """Initialize with all of the e-prints in the record."""
        self.name = name
        self.years = years
