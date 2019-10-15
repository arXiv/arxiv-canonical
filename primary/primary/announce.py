from datetime import datetime, date
from typing import Any, Mapping

from pytz import UTC

from arxiv.canonical import Primary, classic
from arxiv.canonical import domain as D
from arxiv.canonical.services.stream import ProducerEventStream


def announce_from_classic(daily_path: str) -> None:
    """Generate announcement events for today from the classic daily record."""
    # open daily.log
    # parse event data for new, cross, and replacement submissions

    # parse abs file for each event
    # propagate events on the stream


def announce_update() -> None:
    """Announce an update that is outside the classic announcement cycle."""



