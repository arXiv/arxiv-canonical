
import json
import os
from datetime import date
from typing import Optional

import pickle

from arxiv.util.serialize import ISO8601JSONEncoder, ISO8601JSONDecoder
from ..serialize import CanonicalEncoder, CanonicalDecoder


class _Persistent:

    encoder = ISO8601JSONEncoder
    decoder = ISO8601JSONDecoder

    def save(self, path: Optional[str] = None) -> None:
        if path is None:
            path = self._path  # type: ignore ; pylint: disable=no-member
        with open(path, 'w') as f:
            json.dump(self, f, cls=self.encoder)


class PersistentIndex(dict, _Persistent):
    """Persistent lookup with JSON serialization."""

    encoder = ISO8601JSONEncoder
    decoder = ISO8601JSONDecoder

    def load(self, path: str) -> None:
        self._path = path
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump({}, f, cls=self.encoder)
        with open(path, 'r') as f:
            self.update(json.load(f, cls=self.decoder))


class PersistentList(list, _Persistent):
    """Persistent list with JSON serialization."""

    def load(self, path: str) -> None:
        self._path = path
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump([], f, cls=self.encoder)
        with open(path, 'r') as f:
            self.extend(json.load(f, cls=self.decoder))


class PersistentMultifileIndex(dict):
    encoder = ISO8601JSONEncoder
    decoder = ISO8601JSONDecoder

    def load(self, path: str) -> None:
        self._path = path
        if not os.path.exists(self._path):
            os.makedirs(self._path)
        for fname in os.listdir(self._path):
            if not fname.startswith('_'):
                continue
            with open(os.path.join(self._path, fname), 'rb') as f:
                key = json.loads(fname[1:], cls=self.decoder)
                self[key] = pickle.load(f)

    def save(self, path: Optional[str] = None) -> None:
        if path is None:
            path = self._path  # type: ignore ; pylint: disable=no-member
        for key, value in self.items():
            fname = f'_{json.dumps(key, cls=self.encoder)}'
            with open(os.path.join(self._path, fname), 'wb') as f:
                pickle.dump(value, f)
