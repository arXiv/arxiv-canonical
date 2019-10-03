
from typing import Callable, Iterable
from typing_extensions import Protocol

from . import domain as D


class IEventStream(Protocol):
    def emit(self, event: D.Event) -> None:
        ...

    def listen(self, on_event: Callable[[D.Event], None]) -> None:
        ...


# class FixedStream(IEventStream):
#     def __init__(self, event_data: Iterable[Dict[str, Any]])
