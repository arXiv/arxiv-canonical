"""Provides a :class:`.CanonicalEncoder` for domain objects."""

import json
import re

from datetime import datetime, date
from enum import Enum
from typing import Any, Union, List, Dict, Type
from uuid import UUID

from backports.datetime_fromisoformat import MonkeyPatch

from . import classic
from .. import domain


MonkeyPatch.patch_fromisoformat()


def _camel_to_snake(camel: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


class CanonicalEncoder(json.JSONEncoder):
    """Encodes domain objects in this package for serialization."""

    def unpack(self, obj: Any) -> Any:
        """Recursively search for domain objects, and unpack them to dicts."""
        if isinstance(obj, dict):
            return {self.unpack(key): self.unpack(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.unpack(value) for value in obj]
        elif isinstance(obj, domain.CanonicalBase):
            type_snake = _camel_to_snake(type(obj).__name__)
            unpack_obj = getattr(self, f'unpack_{type_snake}',
                                 self.unpack_default)
            data = unpack_obj(obj)
            data['@type'] = type(obj).__name__
            return data
        elif isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, tuple):
            return tuple(self.unpack(value) for value in obj)
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return obj

    def encode(self, obj: Any) -> Any:
        """Serialize objects in this application domain."""
        return super(CanonicalEncoder, self).encode(self.unpack(obj))

    def unpack_default(self, obj: Any) -> Dict:
        """Fallback unpack method for any domain object."""
        return {key: self.unpack(getattr(obj, key))
                for key in obj.__annotations__.keys()}

    def unpack_canonical_file(self, obj: domain.CanonicalFile) -> Dict:
        """Unpack a :class:`.domain.File`."""
        return {key: self.unpack(getattr(obj, key))
                for key in obj.__annotations__.keys() if key != 'content'}

    def unpack_uuid(self, obj: UUID) -> Dict:
        return {'hex': obj.hex}

