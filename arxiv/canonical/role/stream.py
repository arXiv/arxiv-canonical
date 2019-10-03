
from abc import ABC
from typing import Any, List, Optional, Sequence

from .. import domain as D
from ..events import IEventStream
from ..register import ICanonicalSource

from .proxy import EventStreamProxy


class StreamRole(ABC):
    event_supported: List[str] = []

    @property
    def stream(self) -> IEventStream:
        assert self._stream is not None
        return self._stream

    def set_stream(self, stream: IEventStream,
                   sources: Sequence[ICanonicalSource],
                   name: str = 'all') -> None:
        self._stream = EventStreamProxy(stream, self.event_supported)


class NoStream(StreamRole, ABC):
    pass


class Listener(StreamRole, ABC):
    event_supported = ['listen']

    def on_event(self, event: D.Event) -> None:
        raise NotImplementedError('Must be implemented by a child class')


class Emitter(StreamRole, ABC):
    event_supported = ['emit']