from typing import Any, Generic, List, Type, TypeVar

from ..events import IEventStream
from ..register import IRegisterAPI


_Inner = TypeVar('_Inner')


class _BaseProxy(Generic[_Inner]):
    def __init__(self, inner: _Inner, supported: List[str]) -> None:
        self._inner = inner
        self._supported = supported

    def __getattribute__(self, key: str) -> Any:
        if not key.startswith('_'):
            if key in self._supported:
                return getattr(self._inner, key)
            elif hasattr(self._inner, key):
                raise AttributeError(f'{key} is not supported by this proxy')
        return object.__getattribute__(self, key)


class RegisterAPIProxy(_BaseProxy[IRegisterAPI]):
    pass


class EventStreamProxy(_BaseProxy[IEventStream]):
    pass