"""Provides the core announcement event concept."""

from typing import NamedTuple, Type, List, Optional
from datetime import datetime
from enum import Enum

from .identifier import Identifier
from .classification import Classification


class Event(NamedTuple):
    """An announcement-related event."""

    class Type(Enum):
        """Supported event types."""

        NEW = 'new'
        UPDATED = 'updated'
        REPLACED = 'replaced'
        CROSSLIST = 'cross'
        WITHDRAWN = 'withdrawn'

    arxiv_id: Identifier
    event_date: datetime
    event_type: Type
    classifications: List[Classification]

    description: str = ''
    legacy: bool = False
    event_agent: Optional[str] = None
    version: int = -1
