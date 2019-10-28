
from abc import ABC
from typing import Any, Sequence

from .. import domain as D
from ..core import IEventStream, ICanonicalStorage, IRegisterAPI, \
    ICanonicalSource

from .register import Reader, Writer, NoRegister
from .stream import Listener, Emitter, NoStream


class Role(ABC):
    def __init__(self, storage: ICanonicalStorage,
                 sources: Sequence[ICanonicalSource],
                 stream: IEventStream, name: str = 'all') -> None:
        self.set_register(storage, sources, name)
        self.set_stream(stream, sources, name)

    @property
    def register(self) -> IRegisterAPI:
        raise NotImplementedError('Must be implemented by child role')

    @property
    def stream(self) -> IEventStream:
        raise NotImplementedError('Must be implemented by child role')

    def set_register(self, storage: ICanonicalStorage,
                     sources: Sequence[ICanonicalSource],
                     name: str = 'all') -> None:
        raise NotImplementedError('Must be implemented by child role')

    def set_stream(self, stream: IEventStream,
                   sources: Sequence[ICanonicalSource],
                   name: str = 'all') -> None:
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
    def on_event(self, event: D.Event) -> None:
        raise NotImplementedError('Must be implemented by a child class')

