import datetime
from io import BytesIO
from json import dumps, load
from typing import Type, IO, Iterable, Tuple

from .core import RecordEntry, D


class RecordMetadata(RecordEntry[D.Version]):
    """An entry for version metadata."""

    @classmethod
    def make_key(cls, identifier: D.VersionedIdentifier) -> D.Key:
        return D.Key(f'{cls.make_prefix(identifier)}/{identifier}.json')

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
        return (f'e-prints/{ident.year}/{str(ident.month).zfill(2)}/'
                f'{ident.arxiv_id}/v{ident.version}')

    @classmethod
    def from_domain(cls: Type[_Self], version: D.Version,
                    callbacks: Iterable[D.Callback] = ()) -> _Self:
        content, size_bytes = RecordMetadata._encode(version,
                                                     callbacks=callbacks)
        content_type = D.ContentType.json
        key = RecordMetadata.make_key(version.identifier)
        return cls(
            key=key,
            stream=RecordStream(
                domain=D.CanonicalFile(
                    created=version.updated_date,
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
    def _encode(cls, version: D.Version,
                callbacks: Iterable[D.Callback] = ()) -> Tuple[IO[bytes], int]:
        content = dumps(version.to_dict(callbacks=callbacks)).encode('utf-8')
        return BytesIO(content), len(content)

    @classmethod
    def to_domain(self, stream: RecordStream,
                  callbacks: Iterable[D.Callback] = ()) -> D.Version:
        assert stream.content is not None
        version = D.Version.from_dict(load(stream.content), callbacks=callbacks)
        if stream.content.seekable:
            stream.content.seek(0)
        return version  # RecordVersion.post_to_domain(version, load_content)

    @classmethod
    def from_stream(cls, key: D.Key, stream: RecordStream,
                    callbacks: Iterable[D.Callback] = ()) -> 'RecordMetadata':
        return cls(key=key, stream=stream,
                   domain=cls.to_domain(stream, callbacks=callbacks))