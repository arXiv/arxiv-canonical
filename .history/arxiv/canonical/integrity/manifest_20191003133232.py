import json
from enum import Enum
from typing import Optional, List, Dict, Any
from mypy_extensions import TypedDict

from ..domain.version import EventType


class ManifestEntry(TypedDict, total=False):
    key: str
    checksum: Optional[str]
    size_bytes: int
    mime_type: str
    number_of_events: int
    number_of_events_by_type: Dict[EventType, int]
    number_of_versions: int


class Manifest(TypedDict):
    entries: List[ManifestEntry]
    number_of_events: int
    number_of_events_by_type: Dict[EventType, int]
    number_of_versions: int


class ManifestEncoder(json.JSONEncoder):
    def unpack(self, obj: Any) -> Any:
        if isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, dict):
            return {self.unpack(key): self.unpack(value)
                    for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.unpack(item) for item in obj]
        return obj

    def encode(self, obj: Any) -> Any:
        """Serialize manifest objects."""
        return super(ManifestEncoder, self).encode(self.unpack(obj))


class ManifestDecoder(json.JSONDecoder):
    """Decode manifests."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Pass :func:`object_hook` to the base constructor."""
        kwargs['object_hook'] = kwargs.get('object_hook', self.object_hook)
        super(ManifestDecoder, self).__init__(*args, **kwargs)

    def object_hook(self, obj: dict, **extra: Any) -> Any:  # pylint: disable=method-hidden
        """Decode the manifest."""
        if 'number_of_events_by_type' in obj:
            obj['number_of_events_by_type'] = {
                EventType(key): value
                for key, value in obj['number_of_events_by_type'].items()
            }
        return obj


def make_empty_manifest() -> Manifest:
    return Manifest(entries=[],
                    number_of_events=0,
                    number_of_versions=0,
                    number_of_events_by_type={})


def checksum_from_manifest(manifest: Manifest, key: str) -> Optional[str]:
    for entry in manifest['entries']:
        if entry['key'] == key:
            return entry['checksum']
    raise KeyError(f'Not found: {key}')