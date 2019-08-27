"""(De)Serialization of the canonical record."""

from typing import Any
import json

from .encoder import CanonicalEncoder
from .decoder import CanonicalDecoder


def dumps(obj: Any) -> str:
    """Generate JSON from a Python object."""
    return json.dumps(obj, cls=CanonicalEncoder)


def loads(data: str) -> Any:
    """Load a Python object from JSON."""
    return json.loads(data, cls=CanonicalDecoder)
