from typing import Any, Callable, Mapping
from arxiv.canonical.domain import Event
from arxiv.canonical.events import IEventStream
from arxiv.integration.kinesis.consumer import BaseConsumer, process_stream


class EventConsumer(BaseConsumer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(EventConsumer, self).__init__(*args, **kwargs)
        self.on_event: Callable[[Event], None] = kwargs['on_event']

    def process_record(self, record: dict) -> None:
        """Process an event."""
        self.on_event(Event.from_dict(record))


class EventStream(IEventStream):
    """Consumes announcement events, and updates the canonical record."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self._config = config

    def listen(self, on_event: Callable[[Event], None]) -> None:
        process_stream(EventConsumer, self._config,
                       extra=dict(on_event=on_event))
