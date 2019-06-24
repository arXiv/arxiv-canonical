"""Serialization of the canonical record."""

from typing import Any
import json

from .encoder import CanonicalJSONEncoder
from .decoder import CanonicalJSONDecoder


def dumps(obj: Any) -> str:
    """Generate JSON from a Python object."""
    return json.dumps(obj, cls=CanonicalJSONEncoder)


def loads(data: str) -> Any:
    """Load a Python object from JSON."""
    return json.loads(data, cls=CanonicalJSONDecoder)
