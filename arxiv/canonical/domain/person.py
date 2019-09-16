"""Provide person-related domain concepts and logic."""

from typing import Any, Dict, Iterable, List, Optional

from .base import CanonicalBase, Callback, with_callbacks


class Person(CanonicalBase):
    """An arXiv user."""

    full_name: str
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    suffix: Optional[str] = None
    orcid: Optional[str] = None
    author_id: Optional[str] = None
    affiliation: Optional[List[str]] = None

    def __init__(self, full_name: str,
                 last_name: Optional[str] = None,
                 first_name: Optional[str] = None,
                 suffix: Optional[str] = None,
                 orcid: Optional[str] = None,
                 author_id: Optional[str] = None,
                 affiliation: Optional[List[str]] = None) -> None:
        self.full_name = full_name
        self.last_name = last_name
        self.first_name = first_name
        self.suffix = suffix
        self.orcid = orcid
        self.author_id = author_id
        self.affiliation = affiliation

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'Person':
        return cls(
            full_name=data['full_name'],
            last_name=data.get('last_name'),
            first_name=data.get('first_name'),
            suffix=data.get('suffix'),
            orcid=data.get('orcid'),
            author_id=data.get('author_id'),
            affiliation=data.get('affiliation', []),
        )

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) -> Dict[str, Any]:
        return {
            'full_name': self.full_name,
            'last_name': self.last_name,
            'first_name': self.first_name,
            'suffix': self.suffix,
            'orcid': self.orcid,
            'author_id': self.author_id,
            'affiliation': self.affiliation
        }