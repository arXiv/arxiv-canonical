from typing import (Any, Callable, Dict, Iterable, Set, Type, TypeVar, Union,
                    cast)
from typing_extensions import Protocol, runtime_checkable


class CanonicalBase:
    """Base class for all canonical domain classes."""

    exclude_from_comparison: Set[str] = set()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CanonicalBase):
            return False
        keys = ((set(self.__class__.__annotations__.keys())  # pylint: disable=no-member ; subclasses have annotations.
                 | set(other.__class__.__annotations__.keys()))
                - self.exclude_from_comparison)
        try:
            for key in keys:
                assert getattr(self, key) == getattr(other, key)
        except AssertionError:
            return False
        return True


class CanonicalBaseCollection(CanonicalBase):
    """Base class for domain classes that act as collections."""

