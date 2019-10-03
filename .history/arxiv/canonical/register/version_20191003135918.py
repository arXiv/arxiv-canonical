from typing import Optional, Sequence, Type

from .core import Base, D, R, I, ICanonicalStorage, ICanonicalSource, _Self


class RegisterVersion(Base[D.VersionedIdentifier,
                           D.Version,
                           R.RecordVersion,
                           I.IntegrityVersion,
                           str,
                           RegisterFile]):
    domain_type = D.Version
    record_type = R.RecordVersion
    integrity_type = I.IntegrityVersion
    member_type = RegisterFile

    @classmethod
    def create(cls, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               d: D.Version, save_members: bool = True) -> 'RegisterVersion':
        r = R.RecordVersion.from_domain(d, partial(dereference, sources), callbacks=[])  # <- need to dereference URIs here
        i = I.IntegrityVersion.from_record(r)
        members = {}
        for i_member in i.iter_members():
            if isinstance(i_member.record, R.RecordFile):
                assert isinstance(i_member.record.domain, D.CanonicalFile)
                member = RegisterFile(i_member.name,
                                      domain=i_member.record.domain,
                                      record=i_member.record,
                                      integrity=i_member)
            elif isinstance(i_member.record, R.RecordMetadata):
                assert isinstance(i_member.record.domain, D.Version)
                member = RegisterMetadata(i_member.name,
                                          domain=i_member.record.domain,
                                          record=i_member.record,
                                          integrity=i_member)
            if save_members:
                member.save(s)
            members[member.name] = member
        return cls(r.name, domain=d, record=r, integrity=i, members=members)

    @classmethod
    def load(cls: Type[_Self], s: ICanonicalStorage,
             sources: Sequence[ICanonicalSource],
             identifier: D.VersionedIdentifier,
             checksum: Optional[str] = None) -> _Self:
        """
        Load an e-print :class:`.Version` from s.

        This method is overridden since it uses a different member mapping
        struct than higher-level collection types.
        """
        # All of the data needed to reconstitute the Version is in the metadata
        # record.
        key = R.RecordMetadata.make_key(identifier)
        stream, _ = s.load_entry(key)
        d = R.RecordMetadata.to_domain(stream, callbacks=[])   # self.load_deferred
        _r = R.RecordMetadata(key=key, stream=stream, domain=d)

        assert d.source is not None and d.render is not None
        # assert d.source.content is not None and d.render.content is not None
        manifest = s.load_manifest(R.RecordVersion.make_manifest_key(identifier))
        r = R.RecordVersion.from_domain(d, partial(dereference, sources), metadata=_r, callbacks=[])
        i = I.IntegrityVersion.from_record(
            r,
            checksum=checksum,
            calculate_new_checksum=False,
            manifest=manifest
        )
        return cls(date, domain=d, record=r, integrity=i)

    @property
    def member_names(self) -> Set[str]:
        assert self.members is not None
        return set([name for name in self.members])

    @property
    def number_of_events(self) -> int:
        return 0

    @property
    def number_of_versions(self) -> int:
        return 1

    def update(self, s: ICanonicalStorage, sources: Sequence[ICanonicalSource],
               version: D.Version) -> None:
        """
        Update a version in place.

        Removes any members (files) not in the passed ``Version``, and retains
        and ignores members without any content (assumes that this is a partial
        update). Saves any new/changed members, and updates the manifest.
        """
        new_version = self.create(s, sources, version, save_members=False)
        assert self.members is not None and new_version.members is not None
        to_remove = self.member_names - new_version.member_names

        to_add = [name for name in new_version.members
                  # Ignore any members without content, as this may be a
                  # partial update only.
                  if new_version.members[name].domain.content is not None
                    # Select members not already present, or...
                    and (name not in self.members
                         # ...that appear to have changed.
                         or self.members[name].integrity.checksum
                            != new_version.members[name].integrity.checksum)]
        for name in to_remove:
            self.members[name].delete(s)
            del self.members[name]
        altered = set()
        for name in to_add:
            self.members[name] = new_version.members[name]
            altered.add(self.members[name])
        self.save_members(s, altered)   # Updates our manifest.


class RegisterEPrint(Base[D.Identifier,
                          D.EPrint,
                          R.RecordEPrint,
                          I.IntegrityEPrint,
                          D.VersionedIdentifier,
                          RegisterVersion]):
    domain_type = D.EPrint
    record_type = R.RecordEPrint
    integrity_type = I.IntegrityEPrint
    member_type = RegisterVersion

    @classmethod
    def _member_name(cls, event: D.Event) \
            -> Iterable[D.VersionedIdentifier]:
        return [event.version.identifier]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> D.VersionedIdentifier:
        return D.VersionedIdentifier(key)

    def _add_versions(self, s: ICanonicalStorage,
                      sources: Sequence[ICanonicalSource],
                      versions: Iterable[D.Version],
                      fkey: Callable[[D.Version], Any]) \
            -> Iterable[RegisterVersion]:
        assert self.members is not None
        altered = set()
        for version in versions:
            key = fkey(version)
            if key in self.members:
                raise ConsistencyError('Version already exists')
            member = self.member_type.create(s, sources, version)
            self.members[key] = member
            altered.add(member)
        return iter(altered)

    def add_event_new(self, s: ICanonicalStorage,
                      sources: Sequence[ICanonicalSource],
                      event: D.Event) -> List[RegisterVersion]:
        assert self.members is not None
        altered: List[RegisterVersion] = []
        for key in self._member_name(event):
            if key in self.members:
                raise ConsistencyError('Version already exists')
            self.members[key] \
                = self.member_type.create(s, sources, event.version)
            altered.append(self.members[key])
        return altered

    def add_event_update(self, s: ICanonicalStorage,
                         sources: Sequence[ICanonicalSource],
                         event: D.Event) -> List[RegisterVersion]:
        assert self.members is not None
        altered: List[RegisterVersion] = []
        for key in self._member_name(event):
            if key not in self.members:
                raise ConsistencyError(f'No such version: {event.identifier}')
            self.members[key].update(s, sources, event.version)
            altered.append(self.members[key])
        return altered

    def add_event_update_metadata(self, s: ICanonicalStorage,
                                  sources: Sequence[ICanonicalSource],
                                  event: D.Event) -> List[RegisterVersion]:
        if event.version.source is not None:
            assert event.version.source.content is None
        if event.version.render is not None:
            assert event.version.render.content is None
        return self.add_event_update(s, sources, event)

    def add_event_replace(self, s: ICanonicalStorage,
                          sources: Sequence[ICanonicalSource],
                          event: D.Event) -> List[RegisterVersion]:
        return self.add_event_new(s, sources, event)

    def add_event_cross(self, s: ICanonicalStorage,
                        sources: Sequence[ICanonicalSource],
                        event: D.Event) -> List[RegisterVersion]:
        return self.add_event_update_metadata(s, sources, event)

    def add_event_migrate(self, s: ICanonicalStorage,
                          sources: Sequence[ICanonicalSource],
                          event: D.Event) -> List[RegisterVersion]:
        return self.add_event_update(s, sources, event)

    def add_event_migrate_metadata(self, s: ICanonicalStorage,
                                   sources: Sequence[ICanonicalSource],
                                   event: D.Event) -> List[RegisterVersion]:
        return self.add_event_update_metadata(s, sources, event)

    def _add_events(self, s: ICanonicalStorage,
                    sources: Sequence[ICanonicalSource],
                    events: Iterable[D.Event],
                    _: Callable) -> Iterable[RegisterVersion]:
        added: Set[RegisterVersion] = set()
        for event in events:
            adder = getattr(self, f'add_event_{event.event_type.value}', None)
            assert adder is not None
            added |= set(adder(s, sources, event))
        return added


class RegisterDay(Base[date,
                       D.EPrintDay,
                       R.RecordDay,
                       I.IntegrityDay,
                       D.Identifier,
                       RegisterEPrint]):
    domain_type = D.EPrintDay
    record_type = R.RecordDay
    integrity_type = I.IntegrityDay
    member_type = RegisterEPrint

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[D.Identifier]:
        return [event.version.identifier.arxiv_id]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> D.Identifier:
        return D.Identifier(key)


class RegisterMonth(Base[YearMonth,
                         D.EPrintMonth,
                         R.RecordMonth,
                         I.IntegrityMonth,
                         date,
                         RegisterDay]):
    domain_type = D.EPrintMonth
    record_type = R.RecordMonth
    integrity_type = I.IntegrityMonth
    member_type = RegisterDay

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[date]:
        return [event.version.announced_date_first]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> date:
        return datetime.strptime(key, '%Y-%m-%d').date()


class RegisterYear(Base[Year,
                        D.EPrintYear,
                        R.RecordYear,
                        I.IntegrityYear,
                        YearMonth,
                        RegisterMonth]):
    domain_type = D.EPrintYear
    record_type = R.RecordYear
    integrity_type = I.IntegrityYear
    member_type = RegisterMonth

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[YearMonth]:
        return [(event.version.identifier.year,
                 event.version.identifier.month)]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> YearMonth:
        year_part, month_part = key.split('-', 1)
        return int(year_part), int(month_part)


class RegisterEPrints(Base[str,
                           D.AllEPrints,
                           R.RecordEPrints,
                           I.IntegrityEPrints,
                           Year,
                           RegisterYear]):
    domain_type = D.AllEPrints
    record_type = R.RecordEPrints
    integrity_type = I.IntegrityEPrints
    member_type = RegisterYear

    @classmethod
    def _member_name(cls, event: D.Event) -> Iterable[Year]:
        return [event.version.identifier.year]

    @classmethod
    def _manifest_to_member_name(cls, key: str) -> Year:
        return int(key)