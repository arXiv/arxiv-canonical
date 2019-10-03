from .core import Base, D, R, I, ICanonicalStorage, ICanonicalSource


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
        # callbacks = [partial_dereference(sources)]
        r = R.RecordListing.from_domain(d, callbacks=[])
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

            # d = _i.record.to_domain(partial(cls._load_content, s))
            # r = R.RecordListing.from_domain(d)
            #     domain=d,
            #     key=_i.record.key,
            #     content=_i.record.content,
            #     content_type=_i.record.content_type,
            #     size_bytes=_i.record.size_bytes
            # )
            # i = I.IntegrityListing.from_record(
            #     r,
            #     checksum=checksum,
            #     calculate_new_checksum=False
            # )
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