"""Provide license-related domain concepts and logic."""

from typing import Any, Dict, Iterable

from .base import CanonicalBase, Callback, with_callbacks


class License(CanonicalBase):
    """License under which the e-print was provided to arXiv."""

    href: str

    def __init__(self, href: str) -> None:
        self.href = href

    @classmethod
    @with_callbacks
    def from_dict(cls, data: Dict[str, Any],
                  callbacks: Iterable[Callback] = ()) -> 'License':
        return cls(href=data['href'])

    @with_callbacks
    def to_dict(self, callbacks: Iterable[Callback] = ()) \
            -> Dict[str, Any]:
        return {'href': self.href}
