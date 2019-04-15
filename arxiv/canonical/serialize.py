"""JSON serialization for submission core."""

from typing import Any, Union, List
import json
from datetime import datetime, date
from enum import Enum

from backports.datetime_fromisoformat import MonkeyPatch

from . import domain


MonkeyPatch.patch_fromisoformat()


class CanonicalJSONEncoder(json.JSONEncoder):
    """Encodes domain objects in this package for serialization."""

    def unpack(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {key: self.unpack(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.unpack(value) for value in obj]
        elif type(obj) in domain.domain_classes:
            data = {key: self.unpack(value)
                    for key, value in obj._asdict().items()}
            data['@type'] = type(obj).__name__
            return data
        elif isinstance(obj, tuple):
            return tuple(self.unpack(value) for value in obj)
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return obj

    def encode(self, obj: Any) -> Any:
        return super(CanonicalJSONEncoder, self).encode(self.unpack(obj))

    def default(self, obj: Any) -> Any:
        """Serialize objects in this application domain."""
        return super(CanonicalJSONEncoder, self).default(self.unpack(obj))


class CanonicalJSONDecoder(json.JSONDecoder):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Pass :func:`object_hook` to the base constructor."""
        kwargs['object_hook'] = kwargs.get('object_hook', self.object_hook)
        super(CanonicalJSONDecoder, self).__init__(*args, **kwargs)

    def _try_isoparse(self, value: Any) -> Any:
        """Attempt to parse a value as an ISO8601 datetime."""
        if type(value) is not str:
            return value
        try:
            return datetime.fromisoformat(value)  # type: ignore
        except ValueError:
            return value

    def object_hook(self, obj: dict, **extra: Any) -> Any:
        """Decode domain objects in this package."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                if type(value) is list:
                    obj[key] = [self._try_isoparse(v) for v in value]
                else:
                    obj[key] = self._try_isoparse(value)

            obj_type = obj.pop('@type')
            for domain_class in domain.domain_classes:
                if domain_class.__name__ == obj_type:
                    return domain_class(**obj)
        return obj


def dumps(obj: Any) -> str:
    """Generate JSON from a Python object."""
    return json.dumps(obj, cls=CanonicalJSONEncoder)


def loads(data: str) -> Any:
    """Load a Python object from JSON."""
    return json.loads(data, cls=CanonicalJSONDecoder)
