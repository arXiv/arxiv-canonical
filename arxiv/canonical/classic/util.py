
import json
import os
from typing import Optional

from arxiv.util.serialize import ISO8601JSONEncoder, ISO8601JSONDecoder


class _Persistent:
    def save(self, path: Optional[str] = None) -> None:
        if path is None:
            path = self._path  # type: ignore ; pylint: disable=no-member
        with open(path, 'w') as f:
            json.dump(self, f, cls=ISO8601JSONEncoder)

    def __del__(self) -> None:
        try:
            self.save()
        except AttributeError:
            pass


class PersistentIndex(dict, _Persistent):
    """Persistent lookup with JSON serialization."""

    def load(self, path: str) -> None:
        self._path = path
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump({}, f, cls=ISO8601JSONEncoder)
        with open(path, 'r') as f:
            self.update(json.load(f, cls=ISO8601JSONDecoder))


class PersistentList(list, _Persistent):
    """Persistent list with JSON serialization."""

    def load(self, path: str) -> None:
        self._path = path
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump([], f, cls=ISO8601JSONEncoder)
        with open(path, 'r') as f:
            self.extend(json.load(f, cls=ISO8601JSONDecoder))

