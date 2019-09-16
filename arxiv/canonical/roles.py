import datetime
from abc import ABC
from typing import Any, Generic, Iterable, List, Sequence, TypeVar, Union
from typing_extensions import Protocol

from .events import IEventStream
from . import domain as D
from .lock import IWriteLock, WriteLock

from .register import ICanonicalStorage, RegisterAPI, IRegisterAPI, \
    ICanonicalSource


def unsupported(*args: Any) -> None:
    raise RuntimeError(f'Not a supported operation')


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


class RegisterRole(ABC):
    register_supported: List[str] = []

    @property
    def register(self) -> IRegisterAPI:
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
        'load_events'
    ]


class Writer(RegisterRole, ABC):
    register_supported = [
        'add_events',
        'load_listing',
        'load_version',
        'load_eprint',
        'load_history',
        'load_event',
        'load_events'
    ]


class StreamRole(ABC):
    event_supported: List[str] = []

    @property
    def stream(self) -> IEventStream:
        return self._stream

    def set_stream(self, stream: IEventStream, name: str = 'all') -> None:
        self._stream = EventStreamProxy(stream, self.event_supported)


class NoStream(StreamRole, ABC):
    pass


class Listener(StreamRole, ABC):
    event_supported = ['listen']

    def on_event(self, event: D.Event) -> None:
        raise NotImplementedError('Must be implemented by a child class')


class Emitter(StreamRole, ABC):
    event_supported = ['emit']


class Role(ABC):
    def __init__(self, storage: ICanonicalStorage,
                 sources: Sequence[ICanonicalStorage],
                 stream: IEventStream, name: str = 'all') -> None:
        self.set_register(storage, sources, name)
        self.set_stream(stream, name)

    @property
    def register(self) -> IRegisterAPI:
        raise NotImplementedError('Must be implemented by child role')

    @property
    def stream(self) -> IEventStream:
        raise NotImplementedError('Must be implemented by child role')

    def set_register(self, storage: Any, sources: Sequence[ICanonicalStorage],
                     name: str = 'all') -> None:
        raise NotImplementedError('Must be implemented by child role')

    def set_stream(self, stream: IEventStream, name: str = 'all') -> None:
        raise NotImplementedError('Must be implemented by child role')


class Primary(Writer, Emitter, Role):
    """
    The primary canonical record.

    All events are first written to and emitted from this authoritative
    record.
    """
    pass


class Replicant(Writer, Listener, Role):
    """
    A system that transcribes events to a secondary record.

    The primary use-case is for mirror sites.
    """

    def on_event(self, event: D.Event) -> None:
        self.register.add_events(event)


class Repository(Reader, NoStream, Role):
    """A read-only API onto the canonical record."""
    pass


class Observer(NoRegister, Listener, Role):
    """
    A system that processes canonical e-print events.

    Such a system might perform operations in response to canonical events that
    fall outside of the maintenance of the canonical record. For example, it
    might update a secondary index with a subset of data in the event stream.
    """

