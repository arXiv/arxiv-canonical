import datetime
from io import BytesIO
from json import dumps, load
from typing import Type, IO, Iterable, Tuple

from .core import RecordEntry, RecordStream, D, _Self


class RecordMetadata(RecordEntry[D.Version]):
    """An entry for version metadata."""

    @classmethod
    def make_key(cls, identifier: D.VersionedIdentifier) -> D.Key:
        if identifier.is_old_style:
            filename = f'{identifier.numeric_part}v{identifier.version}.json'
        else:
            filename = f'{identifier}.json'
        return D.Key(f'{cls.make_prefix(identifier)}/{filename}')

    @classmethod
    def make_prefix(cls, ident: D.VersionedIdentifier) -> str:
        """
        Make a key prefix for an e-print record.

        Parameters
        ----------
        date : datetime.date
            The day on which the first version of the e-print was announced.
        ident : str
            arXiv identifier

        Returns
        -------
        str

        """
        date_part = f'e-prints/{ident.year}/{str(ident.month).zfill(2)}'
        if ident.is_old_style:
            return f'{date_part}/{ident.category_part}/{ident.numeric_part}/v{ident.version}'
        return f'{date_part}/{ident.arxiv_id}/v{ident.version}'

    @classmethod
    def from_domain(cls: Type[_Self], version: D.Version) -> _Self:
        content, size_bytes = RecordMetadata._encode(version,
                                                     )
        content_type = D.ContentType.json
        key = RecordMetadata.make_key(version.identifier)
        return cls(
            key=key,
            stream=RecordStream(
                domain=D.CanonicalFile(
                    modified=version.updated_date,
                    size_bytes=size_bytes,
                    content_type=content_type,
                    ref=key,
                    filename=key.filename
                ),
                content=content,
                content_type=D.ContentType.json,
                size_bytes=size_bytes
            ),
            domain=version
        )

    @classmethod
    def _encode(cls, version: D.Version) -> Tuple[IO[bytes], int]:
        content = dumps(version.to_dict()).encode('utf-8')
        return BytesIO(content), len(content)

    @classmethod
    def to_domain(cls, stream: RecordStream) -> D.Version:
        assert stream.content is not None
        version = D.Version.from_dict(load(stream.content),
                                      )
        if stream.content.seekable:
            stream.content.seek(0)
        return version  # RecordVersion.post_to_domain(version, load_content)

    @classmethod
    def from_stream(cls, key: D.Key, stream: RecordStream) -> 'RecordMetadata':
        return cls(key=key, stream=stream, domain=cls.to_domain(stream))