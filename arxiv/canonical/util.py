"""Various helpers and utilities that don't belong anywhere else."""

from typing import Dict, Generic, TypeVar

KeyType = TypeVar('KeyType')
ValueType = TypeVar('ValueType')


class GenericMonoDict(Dict[KeyType, ValueType]):
    """A dict with specific key and value types."""

    def __getitem__(self, key: KeyType) -> ValueType: ...