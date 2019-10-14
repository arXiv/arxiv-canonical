from datetime import datetime
from typing import Any, Mapping

from pytz import UTC

from arxiv.canonical import Primary
from arxiv.canonical import domain as D
from arxiv.canonical.services.stream import ProducerEventStream


def announce_from_classic() -> None:
    """Generate announcement events for today from the classic daily record."""
    # open daily.log
    # parse event data for new, cross, and replacement submissions
    # parse abs file for each event
    # propagate events on the stream


def announce_update() -> None:
    """Announce an update that is outside the classic announcement cycle."""



