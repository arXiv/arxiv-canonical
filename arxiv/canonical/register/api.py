"""
Provides the main public API for the canonical register.

See :class:`.RegisterAPI`.
"""

import datetime
from collections import abc
from typing import (Any, Optional, IO, Iterable, Iterator, Union, Sequence,
                    Tuple, overload)

from typing_extensions import Protocol, Literal

from ..manifest import Manifest
from .core import (D, R, I, ICanonicalStorage, ICanonicalSource, Base,
                   Year, Month, YearMonth, IStorableEntry, Selector,
                   IRegisterAPI)
from .eprint import (RegisterEPrint, RegisterDay, RegisterMonth, RegisterYear,
                     RegisterEPrints)
from .exceptions import NoSuchResource, ConsistencyError
from .listing import (RegisterListing, RegisterListingDay,
                      RegisterListingMonth, RegisterListingYear,
                      RegisterListings)
from .metadata import RegisterMetadata
from .version import RegisterVersion

_ID = Union[D.VersionedIdentifier, D.Identifier]


class RegisterAPI(IRegisterAPI):
    """The main public API for the register."""

    def __init__(self, storage: ICanonicalStorage,
                 sources: Sequence[ICanonicalSource],
                 name: str = 'all') -> None:
        """Initialize the API with a storage backend."""
        self._storage = storage
        self._sources = sources
        self._register = Register.load(self._storage, sources, name)

    def add_events(self, *events: D.Event) -> None:
        """Add new events to the register."""
        self._register.add_events(self._storage, self._sources, *events)
        self._register.save(self._storage)

    def load_eprint(self, identifier: D.Identifier) -> D.EPrint:
        """Load an :class:`.EPrint` from the record."""
        eprint = RegisterEPrint.load(self._storage, self._sources, identifier)
        if len(eprint.domain.versions) == 0:
            raise NoSuchResource(f'No versions exist for {identifier}')
        return eprint.domain

    def load_event(self, identifier: str) -> D.Event:
        """Load an :class:`.Event` by identifier."""
        return RegisterListingDay.load_event(self._storage, self._sources,
                                             D.EventIdentifier(identifier))

    def load_events(self, selector: Selector) -> Tuple[Iterable[D.Event], int]:
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

    def load_history(self, identifier: _ID) -> Iterable[D.EventSummary]:
        """Load the event history of an :class:`.EPrint`."""
        if isinstance(identifier, D.Identifier):
            epr = RegisterEPrint.load(self._storage, self._sources, identifier)
            if len(epr.domain.versions) == 0:
                raise NoSuchResource(f'No versions exist for {identifier}')

            return (summary
                    for version in epr.domain.versions
                    for summary in epr.domain.versions[version].events)
        if isinstance(identifier, D.VersionedIdentifier):
            return (summary
                    for summary in self.load_version(identifier).events)
        raise ValueError(f'Cannot load event history for {identifier};'
                         ' invalid type')

    def load_listing(self, date: datetime.date,
                     shard: str = D.Event.get_default_shard()) -> D.Listing:  # pylint: disable=no-member
        """Load a :class:`.Listing` for a particulate date."""
        identifier = D.ListingIdentifier.from_parts(date, shard)
        lst = RegisterListing.load(self._storage, self._sources, identifier)
        return lst.domain

    def load_render(self, identifier: D.VersionedIdentifier) \
            -> Tuple[D.CanonicalFile, IO[bytes]]:
        version = self._load_version(identifier)
        if version.record.render is None \
                or version.record.render.stream.content is None:
            raise NoSuchResource(f'Cannot load render for {identifier}')
        assert version.domain.render is not None
        return version.domain.render, version.record.render.stream.content

    def load_source(self, identifier: D.VersionedIdentifier) \
            -> Tuple[D.CanonicalFile, IO[bytes]]:
        version = self._load_version(identifier)
        if version.record.source.stream.content is None:
            raise NoSuchResource(f'Cannot load source for {identifier}')
        return version.domain.source, version.record.source.stream.content

    def load_version(self, identifier: D.VersionedIdentifier) -> D.Version:
        """Load an e-print :class:`.Version` from the record."""
        return self._load_version(identifier).domain

    def _load_events_date(self, selector: datetime.date) \
            -> Tuple[Iterable[D.Event], int]:
        listing = self.load_listing(selector)
        return ((event for event in listing.events), len(listing.events))

    def _load_events_month(self, selector: YearMonth) \
            -> Tuple[Iterable[D.Event], int]:
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

    def _load_events_year(self, selector: Year) \
            -> Tuple[Iterable[D.Event], int]:
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

    def _load_version(self, identifier: D.VersionedIdentifier) \
            -> RegisterVersion:
        try:
            return RegisterVersion.load(self._storage, self._sources,
                                        identifier)
        except Exception as e:  # TODO: make this more specific.
            raise NoSuchResource(f'No such version: {identifier}') from e


listings_key = Literal['listings']
eprints_key = Literal['eprints']
_TopLevelNames = Union[listings_key, eprints_key]
_TopLevelMembers = Union[RegisterListings, RegisterEPrints]


class Register(Base[str,
                    D.Canon,
                    R.Record,
                    I.Integrity,
                    _TopLevelNames,
                    _TopLevelMembers]):
    domain_type = D.Canon
    record_type = R.Record
    integrity_type = I.Integrity
    member_type = _TopLevelMembers  # type: ignore

    @classmethod
    def _member_name(cls, _: D.Event) -> Iterable[_TopLevelNames]:
        return ['listings', 'eprints']

    @classmethod
    def _get_members(cls, s: ICanonicalStorage,
                     sources: Sequence[ICanonicalSource],
                     _: Manifest) -> '_TopMapping':
        return _TopMapping(RegisterListings.load(s, sources, 'listings'),
                           RegisterEPrints.load(s, sources, 'eprints'))

    def add_events(self, s: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   *events: D.Event) -> None:
        """Add events to this register."""
        for event in events:
            event.version.events.append(event.summary)
        super(Register, self).add_events(s, sources, *events)


class _TopMapping(abc.MutableMapping):
    def __init__(self, listings: RegisterListings,
                 eprints: RegisterEPrints) -> None:
        """Initilize with listings and eprints registers."""
        self.eprints = eprints
        self.listings = listings

    @overload
    def __getitem__(self, obj: listings_key) -> RegisterListings: ...
    @overload
    def __getitem__(self, obj: eprints_key) -> RegisterEPrints: ...  # pylint: disable=function-redefined
    def __getitem__(self, obj: Any) -> Any:  # pylint: disable=function-redefined
        if obj == 'eprints':
            return self.eprints
        if obj == 'listings':
            return self.listings
        raise KeyError('No such resource')

    def __delitem__(self, obj: Any) -> None:
        raise NotImplementedError('Does not support deletion')

    def __setitem__(self, obj: Any, value: Any) -> None:
        if obj == 'eprints' and isinstance(value, RegisterEPrints):
            self.eprints = value
        if obj == 'listings' and isinstance(value, RegisterListings):
            self.listings = value
        raise ValueError('Not supported')

    def __iter__(self) -> Iterator[Any]:
        return iter([self.eprints, self.listings])

    def __len__(self) -> int:
        return 2