"""
Provides the main public API for the canonical register.

See :class:`.RegisterAPI`.
"""

import datetime
from typing import Optional, Iterable, Union, Sequence, Tuple

from typing_extensions import Protocol

from .core import D, ICanonicalStorage, ICanonicalSource

Year = int
Month = int
YearMonth = Tuple[Year, Month]


class IRegisterAPI(Protocol):
    def add_events(self, *events: Event) -> None:
        """Add new events to the register."""
        ...

    def load_version(self, identifier: Union[str, D.VersionedIdentifier]) \
            -> Version:
        """Load an e-print :class:`.Version` from the record."""
        ...

    def load_eprint(self, identifier: D.Identifier) -> D.EPrint:
        """Load an :class:`.EPrint` from the record."""
        ...

    def load_history(self,
                     identifier: Union[D.VersionedIdentifier, D.Identifier, str]) \
            -> Iterable[EventSummary]:
        """Load the event history of an :class:`.EPrint`."""
        ...

    def load_event(self, identifier: str) -> D.Event:
        """Load an :class:`.Event` by identifier."""
        ...

    def load_events(self, selector: Union[Year, YearMonth, datetime.date]) \
            -> Tuple[Iterable[Event], int]:
        """Load all :class:`.Event`s for a day, month, or year."""
        ...

    def load_listing(self, date: datetime.date,
                     shard: str = D.Event.get_default_shard()) -> D.Listing:  # pylint: disable=no-member
        """Load a :class:`.Listing` for a particulate date."""
        ...


class RegisterAPI(IRegisterAPI):
    """The main public API for the register."""

    def __init__(self, storage: ICanonicalStorage,
                 sources: Sequence[ICanonicalSource],
                 name: str = 'all') -> None:
        """Initialize the API with a storage backend."""
        self._storage = storage
        self._sources = sources
        self._register = Register.load(self._storage, sources, name)

    def add_events(self, *events: Event) -> None:
        """Add new events to the register."""
        self._register.add_events(self._storage, self._sources, *events)
        self._register.save(self._storage)

    def load_version(self, identifier: Union[str, VersionedIdentifier]) \
            -> Version:
        """Load an e-print :class:`.Version` from the record."""
        if not isinstance(identifier, VersionedIdentifier):
            identifier = VersionedIdentifier(identifier)
        ver = RegisterVersion.load(self._storage, self._sources, identifier)
        return ver.domain

    def load_eprint(self, identifier: Identifier) -> EPrint:
        """Load an :class:`.EPrint` from the record."""
        eprint = RegisterEPrint.load(self._storage, self._sources, identifier)
        return eprint.domain

    def load_history(self,
                     identifier: Union[VersionedIdentifier, Identifier, str]) \
            -> Iterable[EventSummary]:
        """Load the event history of an :class:`.EPrint`."""
        if not isinstance(identifier, (VersionedIdentifier, Identifier)):
            try:
                identifier = VersionedIdentifier(identifier)
            except ValueError:
                identifier = Identifier(identifier)

        if isinstance(identifier, Identifier):
            epr = RegisterEPrint.load(self._storage, self._sources, identifier)
            return (summary
                    for version in epr.domain.versions
                    for summary in epr.domain.versions[version].events)
        if isinstance(identifier, VersionedIdentifier):
            return (summary
                    for summary in self.load_version(identifier).events)
        raise ValueError(f'Cannot load event history for {identifier};'
                         ' invalid type')

    def load_event(self, identifier: str) -> Event:
        """Load an :class:`.Event` by identifier."""
        return RegisterListingDay.load_event(self._storage, self._sources,
                                             EventIdentifier(identifier))

    def load_events(self, selector: Union[Year, YearMonth, datetime.date]) \
            -> Tuple[Iterable[Event], int]:
        """
        Load all :class:`.Event`s for a day, month, or year.

        Returns an :class:`.Event` generator that loads event data lazily from
        the underlying storage, so that in general we are loading only the data
        that we are actually consuming. Events are generated in order.

        **But be warned!** Evaluating the entire generator all at once (e.g. by
        coercing it to a ``list``) may load a considerable amount of data into
        memory (and use a lot of i/o), especially if events for an entire year
        are loaded.

        Parameters
        ----------
        selector : int, tuple, or :class:`datetime.date`
            Indicates the year (int), month (Tuple[int, int]), or day for which
            events should be loaded.

        Returns
        -------
        generator
            Yields :class:`.Event` instances in chronological order.
        int
            An estimate of the number of events that will be generated. Note
            that the actual number may change (especially for large selections)
            because the record may be updated while the generator is being
            consumed.

        """
        if isinstance(selector, datetime.date):
            return self._load_events_date(selector)
        if isinstance(selector, tuple):
            return self._load_events_month(selector)
        if isinstance(selector, Year):
            return self._load_events_year(selector)
        raise ValueError(f'Cannot load events for {selector}; invalid type')

    def load_listing(self, date: datetime.date,
                     shard: str = Event.get_default_shard()) -> Listing:  # pylint: disable=no-member
        """Load a :class:`.Listing` for a particulate date."""
        identifier = ListingIdentifier.from_parts(date, shard)
        lst = RegisterListing.load(self._storage, self._sources, identifier)
        return lst.domain

    def _load_events_date(self, selector: datetime.date) \
            -> Tuple[Iterable[Event], int]:
        listing = self.load_listing(selector)
        return ((event for event in listing.events), len(listing.events))

    def _load_events_month(self, selector: YearMonth) \
            -> Tuple[Iterable[Event], int]:
        assert len(selector) == 2
        assert isinstance(selector[0], int), isinstance(selector[1], int)
        listing_month = RegisterListingMonth.load(self._storage, self._sources,
                                                  selector)
        return (
            (event
             for listing_day in listing_month.iter_members()
             for listing in listing_day.iter_members()
             for event in listing.record.domain.events),
            listing_month.number_of_events
        )

    def _load_events_year(self, selector: Year) -> Tuple[Iterable[Event], int]:
        listing_year = RegisterListingYear.load(self._storage, self._sources,
                                                selector)
        return (
            (event
             for listing_month in listing_year.iter_members()
             for listing_day in listing_month.iter_members()
             for listing in listing_day.iter_members()
             for event in listing.record.domain.events),
            listing_year.number_of_events
        )
