"""Provide person-related domain concepts and logic."""

from typing import NamedTuple, Optional, List


class Person(NamedTuple):
    """An arXiv user."""

    full_name: str
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    suffix: Optional[str] = None
    orcid: Optional[str] = None
    author_id: Optional[str] = None
    affiliation: Optional[List[str]] = None