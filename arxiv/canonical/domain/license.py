"""Provide license-related domain concepts and logic."""

from typing import Any, Dict, Iterable

from .base import CanonicalBase


class License(CanonicalBase):
    """License under which the e-print was provided to arXiv."""

    href: str

    def __init__(self, href: str) -> None:
        self.href = href

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'License':
        return cls(href=data['href'])

    def to_dict(self) -> Dict[str, Any]:
        return {'href': self.href}
