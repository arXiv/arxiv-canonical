
from abc import ABC
from typing import Any, List, Optional, Sequence

from ..register import ICanonicalStorage, RegisterAPI, IRegisterAPI, \
    ICanonicalSource
from .proxy import RegisterAPIProxy


class RegisterRole(ABC):
    register_supported: List[str] = []

    @property
    def register(self) -> IRegisterAPI:
        assert self._register is not None
        return self._register

    def set_register(self, storage: Any, sources: Sequence[ICanonicalSource],
                     name: str = 'all') -> None:
        self._register = RegisterAPIProxy(RegisterAPI(storage, sources, name),
                                          self.register_supported)


class NoRegister(RegisterRole, ABC):
    pass


class Reader(RegisterRole, ABC):
    register_supported = [
        'load_listing',
        'load_version',
        'load_eprint',
        'load_history',
        'load_event',
        'load_events',
        'load_source',
        'load_render'
    ]


class Writer(Reader, ABC):
    register_supported = Reader.register_supported + [
        'add_events',
    ]