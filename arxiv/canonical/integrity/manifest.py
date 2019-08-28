from typing import Optional, List
from mypy_extensions import TypedDict


class ManifestEntry(TypedDict, total=False):
    key: str
    checksum: Optional[str]
    size_bytes: int
    mime_type: str


class Manifest(TypedDict):
    entries: List[ManifestEntry]