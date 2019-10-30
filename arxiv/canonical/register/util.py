
import collections
from uuid import uuid4
from typing import Callable, MutableMapping, Any, Dict, Iterator, List, \
    Sequence


class LazyMapView(collections.abc.MutableMapping):
    def __init__(self, mapping: MutableMapping[Any, Any],
                 getter: Callable[[Any], Any]) -> None:
        self._mapping = mapping
        self._getter = getter

    def __len__(self) -> int:
        return len(self._mapping)

    def __getitem__(self, key: Any) -> Any:
        return self._getter(self._mapping[key])

    def __iter__(self) -> Iterator[Any]:
        return iter(self._mapping)

    def __delitem__(self, key: Any) -> None:
        raise NotImplementedError('Deletion is not allowed')

    def __setitem__(self, key: Any, value: Any) -> None:
        raise NotImplementedError('not yet')


class LazyMap(collections.abc.MutableMapping):
    def __init__(self, keys: List[Any], load: Callable[[Any], Any],
                 strict: bool = False) -> None:
        self._keys = keys
        self._load = load
        self._data: Dict[Any, Any] = {}
        self._strict = strict

    def __getitem__(self, key: Any) -> Any:
        if self._strict and key not in self._keys:
            raise KeyError(f'No such key: {key}')
        try:
            if key not in self._data:
                self._data[key] = self._load(key)
            return self._data[key]
        except Exception as e:
            raise KeyError(f'{key} not found or not supported') from e

    def __len__(self) -> int:
        return len(self._keys)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._keys)

    def __contains__(self, key: Any) -> bool:
        return bool(key in self._keys)

    def __delitem__(self, key: Any) -> None:
        raise NotImplementedError('Deletion is not allowed')

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value
        self._keys.append(key)