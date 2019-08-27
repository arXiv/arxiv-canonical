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
    def __init__(self, keys: List[Any], load: Callable[[Any], Any]) -> None:
        self._keys = keys
        self._load = load
        self._data: Dict[Any, Any] = {}

    def __getitem__(self, key: Any) -> Any:
        # if key not in self._keys:
        #     raise KeyError(f'No such key: {key}')
        if key not in self._data:
            self._data[key] = self._load(key)
        return self._data[key]

    def __len__(self) -> int:
        return len(self._keys)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._keys)

    def __delitem__(self, key: Any) -> None:
        raise NotImplementedError('Deletion is not allowed')

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value
        self._keys.append(key)


class LazySequenceView(collections.abc.Sequence):
    def __init__(self, seq: Sequence, getter: Callable[[Any], Any]) -> None:
        self._getter = getter
        self._seq = seq

    def __len__(self) -> int:
        return len(self._seq)

    def __getitem__(self, idx: Any) -> Any:
        if not isinstance(idx, int):
            raise TypeError('Only integer indices are supported')
        return self._getter(self._seq[idx])

    def __iter__(self) -> Iterator[Any]:
        return (self._getter(o) for o in self._seq)


class LazySequence(collections.abc.MutableSequence):
    def __init__(self, keys: List[Any],
                 load: Callable[[Any], Any]) -> None:
        self._keys = keys  # Keys that define the shape and content of the seq.
        self._values: Dict[Any, Any] = {}   # Maps unique keys to values.
        self._items: Dict[int, Any] = {}    # Maps index to values.
        self._load = load  # Loads a value for a key.

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, idx: Any) -> Any:
        if not isinstance(idx, int):
            raise TypeError('Only integer indices are supported')
        if idx not in self._items:
            self._items[idx] = self.get(self._keys[idx])
        return self._items[idx]

    def __iter__(self) -> Iterator[Any]:
        return (self[i] for i in range(len(self)))

    def __delitem__(self, key: Any) -> None:
        raise NotImplementedError('Deletion is not allowed')

    def __setitem__(self, key: Any, value: Any) -> None:
        raise NotImplementedError('not yet')

    def insert(self, idx: Any, value: Any) -> None:
        if not isinstance(idx, int):
            raise TypeError('Only integer indices are supported')
        key = str(uuid4())  # All that matters is that the key is unique.
        self._values[key] = value
        self._items[idx] = value
        self._keys.insert(idx, key)

    def get(self, key: Any) -> Any:
        if key not in self._values:
            self._values[key] = self._load(key)
        return self._values[key]