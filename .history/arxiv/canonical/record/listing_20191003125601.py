
import datetime
from io import BytesIO
from json import dumps, load
from typing import Type, IO, Iterable, Tuple

from .core import RecordEntry, RecordStream, D, _Self


class RecordListing(RecordEntry[D.Listing]):
    """A listing entry."""

    @classmethod
    def from_domain(cls: Type[_Self], listing: D.Listing,
                    callbacks: Iterable[D.Callback] = ()) -> _Self:
        """Serialize a :class:`.Listing`."""
        content, size_bytes = cls._encode(listing, callbacks=callbacks)
        key = cls.make_key(listing.identifier)
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

    @staticmethod
    def _encode(listing: D.Listing,
                callbacks: Iterable[D.Callback] = ()) -> Tuple[IO[bytes], int]:
        content = dumps(listing.to_dict(callbacks=callbacks)).encode('utf-8')
        return BytesIO(content), len(content)

    @property
    def created(self) -> datetime.datetime:
        return self.domain.start_datetime

    @property
    def name(self) -> str:
        return 'listing'