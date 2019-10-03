import os
from datetime import date, datetime
from functools import partial
from typing import Any, Callable, Iterable, List, Optional, Set, Sequence, Type

from .core import (Base, D, R, I, ICanonicalStorage, ICanonicalSource, _Self,
                   Year, Month, YearMonth, dereference)
from .exceptions import NoSuchResource


class RegisterListing(Base[D.ListingIdentifier,
                           D.Listing,
                           R.RecordListing,
                           I.IntegrityListing,
                           None,
                           None]):

    domain_type = D.Listing
    record_type = R.RecordListing
    integrity_type = I.IntegrityListing
    member_type = type(None)

    @classmethod
    def create(cls, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               d: D.Listing) -> 'RegisterListing':
        r = R.RecordListing.from_domain(d)
        i = I.IntegrityListing.from_record(r)
        s.store_entry(i)
        return cls(d.identifier, domain=d, record=r, integrity=i)

    @classmethod
    def load(cls: Type[_Self], s: ICanonicalStorage,
             sources: Sequence[ICanonicalSource],
             identifier: D.ListingIdentifier,
             checksum: Optional[str] = None) -> _Self:

        try:
            key = R.RecordListing.make_key(identifier)
            stream, _checksum = s.load_entry(key)

            d = R.RecordListing.to_domain(stream, callbacks=[])
            r = R.RecordListing(key=key, stream=stream, domain=d)
            if checksum is not None:
                assert checksum == _checksum
            i = I.IntegrityListing.from_record(r, checksum=_checksum,
                                               calculate_new_checksum=False)
        except Exception:
            d = D.Listing(identifier, events=[])
            r = R.RecordListing.from_domain(d, callbacks=[])
            i = I.IntegrityListing.from_record(r)
        return cls(identifier, domain=d, integrity=i, record=r)

    @property
    def number_of_events(self) -> int:
        return self.domain.number_of_events

    @property
    def number_of_versions(self) -> int:
        return self.domain.number_of_versions

    def add_events(self, _: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   *events: D.Event) -> None:
        """
        Add events to the terminal listing R.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        N = len(events)
        for i, event in enumerate(events):
            self.domain.events.insert(N + i, event)
        self.record = R.RecordListing.from_domain(self.domain,
                                                  callbacks=[])
        self.integrity = I.IntegrityListing.from_record(self.record)

    def save(self, s: ICanonicalStorage) -> str:
        """
        Save this file.

        Overrides the base method since this is a terminal record, not a
        collection.
        """
        s.store_entry(self.integrity)
        self.integrity.update_checksum()
        return self.integrity.checksum

    def delete(self, s: ICanonicalStorage) -> None:
        raise NotImplementedError('not yet; do this please')


class RegisterListingDay(Base[date,
                              D.ListingDay,
                              R.RecordListingDay,
                              I.IntegrityListingDay,
                              D.ListingIdentifier,
                              RegisterListing]):
    domain_type = D.ListingDay
    record_type = R.RecordListingDay
    integrity_type = I.IntegrityListingDay
    member_type = RegisterListing

    @classmethod
    def _member_name(cls, event: D.Event) \
            -> Iterable[D.ListingIdentifier]:
        return [D.ListingIdentifier.from_parts(event.event_date.date(),
                                                    event.event_id.shard)]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> D.ListingIdentifier:
        # return ListingIdentifier(key)
        base, term = os.path.split(key)
        term, _ = os.path.splitext(term)
        y, m, d, shrd = term.split('-', 3)
        return D.ListingIdentifier.from_parts(date(int(y), int(m),
                                                   int(d)), shrd)

    @classmethod
    def load_event(cls, s: ICanonicalStorage,
                   sources: Sequence[ICanonicalSource],
                   identifier: D.EventIdentifier) -> D.Event:
        listing = cls.load(s, sources, identifier.event_date)
        for member in listing.members:
            for event in listing.members[member].domain.events:
                if event.event_id == identifier:
                    return event
        raise NoSuchResource(f'No such event: {identifier}')

    def add_listing(self, s: ICanonicalStorage,
                    sources: Sequence[ICanonicalSource],
                    d: D.Listing) -> None:
        assert self.members is not None
        member = RegisterListing.create(s, sources, d)
        self.members[member.domain.identifier] = member
        self.integrity.extend_manifest(member.integrity)


class RegisterListingMonth(Base[YearMonth,
                                D.ListingMonth,
                                R.RecordListingMonth,
                                I.IntegrityListingMonth,
                                date,
                                RegisterListingDay]):

    domain_type = D.ListingMonth
    record_type = R.RecordListingMonth
    integrity_type = I.IntegrityListingMonth
    member_type = RegisterListingDay

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[date]:
        return [event.event_date.date()]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> date:
        return datetime.strptime(key, '%Y-%m-%d').date()


class RegisterListingYear(Base[Year,
                               D.ListingYear,
                               R.RecordListingYear,
                               I.IntegrityListingYear,
                               YearMonth,
                               RegisterListingMonth]):
    domain_type = D.ListingYear
    record_type = R.RecordListingYear
    integrity_type = I.IntegrityListingYear
    member_type = RegisterListingMonth

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[YearMonth]:
        return [(event.event_date.year, event.event_date.month)]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> YearMonth:
        year_part, month_part = key.split('-', 1)
        return int(year_part), int(month_part)


class RegisterListings(Base[str,
                            D.AllListings,
                            R.RecordListings,
                            I.IntegrityListings,
                            Year,
                            RegisterListingYear]):
    domain_type = D.AllListings
    record_type = R.RecordListings
    integrity_type = I.IntegrityListings
    member_type = RegisterListingYear

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[Year]:
        return [event.event_date.year]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> Year:
        return int(key)