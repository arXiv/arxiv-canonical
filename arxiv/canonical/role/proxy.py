from typing import Any, List

from ..events import IEventStream
from ..register import IRegisterAPI


class RegisterAPIProxy(IRegisterAPI):
    def __init__(self, register: IRegisterAPI, supported: List[str]) -> None:
        self._register = register
        self._supported = supported

    def __getattribute__(self, key: str) -> Any:
        if not key.startswith('_') and key in self._supported:
            return getattr(self._register, key)
        return object.__getattribute__(self, key)


class EventStreamProxy(IEventStream):
    def __init__(self, stream: IEventStream, supported: List[str]) -> None:
        self._stream = stream
        self._supported = supported

    def __getattribute__(self, key: str) -> Any:
        if not key.startswith('_') and key in self._supported:
            return getattr(self._stream, key)
        return object.__getattribute__(self, key)