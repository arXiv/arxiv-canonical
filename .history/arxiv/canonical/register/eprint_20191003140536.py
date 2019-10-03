
from datetime import date, datetime
from typing import Any, Callable, Iterable, List, Optional, Set, Sequence, Type

from .core import (Base, D, R, I, ICanonicalStorage, ICanonicalSource, _Self,
                   Year, Month, YearMonth, dereference)
from .exceptions import ConsistencyError
from .file import RegisterFile
from .metadata import RegisterMetadata
from .version import RegisterVersion


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
    def _member_name(cls, event: D.Event) -> Iterable[D.VersionedIdentifier]:
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