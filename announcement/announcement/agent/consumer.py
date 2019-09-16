"""
E-print event consumer.

In v0 of the announcement agent, the e-print event consumer processes
notifications about announcement events generated by the legacy system, and
updates its version of the canonical record.

## Events

The legacy system produces e-print events on a Kinesis stream called
``Announce``. Each message has the structure:

```json
{
    "event_type": "...",
    "identifier": "...",
    "version": "...",
    "timestamp": "..."
}
```

``event_type`` may be one of:

| Event type | Description                                                   |
|------------|---------------------------------------------------------------|
| new        | An e-print is announced for the first time.                   |
| updated    | An e-print is updated without producing a new version.        |
| replaced   | A new version of an e-print is announced.                     |                                             |
| cross-list | Cross-list classifications are added for an e-print.          |
| withdrawn  | An e-print is withdrawn. This generates a new version.        |

``identifier`` is an arXiv identifier; see :class:`.Identifier`.

``version`` is a positive integer.

``timestamp`` is an ISO-8601 datetime, localized to UTC.

## Implementation

This Kinesis consumer should use the base classes provided by
:mod:`arxiv.integration.kinesis.consumer`
(https://github.com/arXiv/arxiv-base/blob/master/arxiv/integration/kinesis/consumer/__init__.py).

"""
from typing import Any

from arxiv.integration.kinesis.consumer import BaseConsumer

from arxiv.canonical.services import CanonicalStore

from ..services import LegacyMetadataService, LegacyPDFService, \
        LegacySourceService


class AnnouncementConsumer(BaseConsumer):
    """Consumes announcement events, and updates the canonical record."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(AnnouncementConsumer, self).__init__(*args, **kwargs)
        self._metadata_service = LegacyMetadataService.current_session()
        self._pdf_service = LegacyPDFService.current_session()
        self._source_service = LegacySourceService.current_session()
        self._store = CanonicalStore.current_session()

    def process_record(self, record: dict) -> None:
        """Process an announcement record."""
        raise NotImplementedError('Implement me!')