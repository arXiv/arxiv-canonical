
import datetime
from io import BytesIO
from json import dumps, load
from typing import Type, IO, Iterable, Tuple

from .core import RecordBase, RecordEntry, RecordStream, D, _Self, \
    Year, YearMonth


class RecordListing(RecordEntry[D.Listing]):
    """A listing entry."""

    @classmethod
    def from_domain(cls: Type[_Self], listing: D.Listing,
                    callbacks: Iterable[D.Callback] = ()) -> _Self:
        """Serialize a :class:`.Listing`."""
        content, size_bytes = RecordListing._encode(listing, callbacks=callbacks)
        key = RecordListing.make_key(listing.identifier)
        return cls(
            key=key,
            stream=RecordStream(
                domain=D.CanonicalFile(
                    created=listing.start_datetime,
                    modified=listing.end_datetime,
                    size_bytes=size_bytes,
                    content_type=D.ContentType.json,
                    # content=content,
                    filename=key.filename,
                    ref=key
                ),
                content=content,
                content_type=D.ContentType.json,
                size_bytes=size_bytes
            ),
            domain=listing
        )

    @classmethod
    def from_stream(cls, key: D.Key, stream: RecordStream,
                    callbacks: Iterable[D.Callback] = ()) -> 'RecordListing':
        return cls(key=key, stream=stream,
                   domain=cls.to_domain(stream, callbacks=callbacks))

    @classmethod
    def make_key(cls, identifier: D.ListingIdentifier) -> D.Key:
        prefix = cls.make_prefix(identifier.date)
        value: str = identifier.date.strftime(
            f'{prefix}/%Y-%m-%d-{identifier.name}.json'
        )
        return D.Key(value)

    @classmethod
    def make_prefix(cls, date: datetime.date) -> str:
        return date.strftime(f'announcement/%Y/%m/%d')

    @classmethod
    def to_domain(cls, stream: RecordStream,
                  callbacks: Iterable[D.Callback] = ()) -> D.Listing:
        assert stream.content is not None
        listing = D.Listing.from_dict(load(stream.content),
                                      callbacks=callbacks)
        if stream.content.seekable:
            stream.content.seek(0)
        return listing

    @classmethod
    def _encode(cls, listing: D.Listing,
                callbacks: Iterable[D.Callback] = ()) -> Tuple[IO[bytes], int]:
        content = dumps(listing.to_dict(callbacks=callbacks)).encode('utf-8')
        return BytesIO(content), len(content)

    @property
    def created(self) -> datetime.datetime:
        return self.domain.start_datetime

    @property
    def name(self) -> str:
        return 'listing'


class RecordListingDay(RecordBase[datetime.date,
                                  D.ListingIdentifier,
                                  RecordListing,
                                  D.ListingDay]):

    @classmethod
    def make_manifest_key(cls, date: datetime.date) -> D.Key:
        return D.Key(date.strftime('announcement/%Y/%m/%Y-%m-%d.manifest.json'))


class RecordListingMonth(RecordBase[YearMonth,
                                    datetime.date,
                                    RecordListing,
                                    D.ListingMonth]):
    @classmethod
    def make_manifest_key(cls, year_and_month: YearMonth) -> D.Key:
        """
        Make a key for a monthly listing manifest.

        Returns
        -------
        str

        """
        yr, month = year_and_month
        return D.Key(f'announcement/{yr}/{yr}-{str(month).zfill(2)}.manifest.json')


class RecordListingYear(RecordBase[Year,
                                   YearMonth,
                                   RecordListingMonth,
                                   D.ListingYear]):

    @classmethod
    def make_manifest_key(cls, year: Year) -> D.Key:
        """
        Make a key for a yearly listing manifest.

        Returns
        -------
        str

        """
        return D.Key(f'announcement/{year}.manifest.json')


class RecordListings(RecordBase[str, Year, RecordListingYear, D.AllListings]):

    @classmethod
    def make_manifest_key(cls, _: str) -> D.Key:
        """
        Make a key for a root listing manifest.

        Returns
        -------
        str

        """
        return D.Key('announcement.manifest.json')