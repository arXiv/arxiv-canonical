from enum import Enum


class ContentType(Enum):
    pdf = 'pdf'
    targz = 'targz'
    json = 'json'
    abs = 'abs'

    @property
    def mime_type(self) -> str:
        return _mime_types[self]

    @property
    def ext(self) -> str:
        return _extensions[self]

    @classmethod
    def from_mimetype(cls, mime: str) -> 'ContentType':
        return {v: k for k, v in _mime_types.items()}[mime]


_mime_types = {
    ContentType.pdf: 'application/pdf',
    ContentType.targz: 'application/gzip',
    ContentType.json: 'application/json',
    ContentType.abs: 'text/plain',
}

_extensions = {
    ContentType.pdf: 'pdf',
    ContentType.targz: 'tar.gz',
    ContentType.json: 'json',
    ContentType.abs: 'abs',
}