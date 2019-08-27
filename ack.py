from typing import NamedTuple, Any, Generic, TypeVar, NamedTupleMeta, GenericMeta, Type, Dict, Iterable, Tuple
from operator import attrgetter
from typing_extensions import Protocol

class BaseDomain:
    def __init__(self, bat: int) -> None:
        self.bat = bat


Domain = TypeVar('Domain', bound=BaseDomain)
Record = TypeVar('Record')
Integrity = TypeVar('Integrity')


class GenericRegister(Generic[Domain]):
    def __init__(self, domain: Domain) -> None:
        self.domain = domain


class FooDomain(BaseDomain):
    pass

class FooRegister(GenericRegister[FooDomain]):
    pass


foo = FooRegister(FooDomain(3))

reveal_type(foo.domain)


def get_it(reg: GenericRegister[Domain]) -> Domain:
    return reg.domain


reveal_type(get_it(foo))