from json import dumps
from typing import Any, Callable, Dict, Optional

import boto3

from arxiv.canonical.domain import Event
from arxiv.canonical.events import IEventStream
from arxiv.integration.kinesis.consumer import BaseConsumer, process_stream


class EventConsumer(BaseConsumer):
    """Consumes announcement events, and updates the canonical record."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(EventConsumer, self).__init__(*args, **kwargs)
        self.on_event: Callable[[Event], None] = kwargs['on_event']

    def process_record(self, record: dict) -> None:
        """Process an event."""
        self.on_event(Event.from_dict(record))


class ListenerEventStream(IEventStream):
    Consumer = EventConsumer

    def __init__(self, config: Dict[str, Any],
                 on_event: Callable[[Event], None]) -> None:
        self._on_event = on_event
        self._config = config
        self._config['on_event'] = self._on_event

    def listen(self, on_event: Callable[[Event], None]) -> None:
        process_stream(self.Consumer, self._config,
                       extra=dict(on_event=on_event))


# TODO: arxiv.base.integration.kinesis should have a BaseKinesis class with
# the bulk of the boto3 integration. This will be OK for now.
class _Producer(BaseConsumer):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._last_sequence_number: Optional[str] \
            = kwargs.pop('last_sequence_number', None)
        super(_Producer, self).__init__(*args, **kwargs)
        self.client = self.new_client()
        self.get_or_create_stream()


    def emit(self, payload: bytes) -> None:
        # SequenceNumberForOrdering for must be the SequenceNumber of the
        # last record that was produced on this partition.
        if self._last_sequence_number is not None:
            response = self.client.put_record(
                StreamName=self.stream_name,
                Data=payload,
                PartitionKey='',
                SequenceNumberForOrdering=self._last_sequence_number
            )
        else:
            response = self.client.put_record(
                StreamName=self.stream_name,
                Data=payload,
                PartitionKey=''
            )
        self._last_sequence_number = response['SequenceNumber']


class ProducerEventStream(IEventStream):

    def __init__(self, config: Dict[str, Any]) -> None:
        self._producer = _Producer(**config)

    def emit(self, event: Event) -> None:
        self._producer.emit(dumps(event.to_dict()).encode('utf-8'))
