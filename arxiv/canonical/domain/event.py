"""Provides the core announcement event concept."""

from typing import NamedTuple, Type, List, Optional
from datetime import datetime
from enum import Enum

from arxiv.taxonomy import Category
from .identifier import Identifier


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
    categories: List[Category]

    description: str = ''
    legacy: bool = False
    event_agent: Optional[str] = None
    version: int = -1
