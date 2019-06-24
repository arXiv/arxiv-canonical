"""Provide classification-related domain concepts and logic."""

from typing import NamedTuple, Optional
from arxiv.taxonomy import Category, Archive


class Classification(NamedTuple):
    archive: Archive
    category: Optional[Category] = None