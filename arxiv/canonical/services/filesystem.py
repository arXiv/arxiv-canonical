import gzip
import io
import json
import logging
import os
from datetime import datetime
from http import HTTPStatus as status
from typing import IO, Iterable, List, Tuple, Union
from urllib3.util.retry import Retry

import requests
from pytz import timezone, UTC

from .. import domain as D
from .. import record as R
from .. import integrity as I
from ..manifest import Manifest, ManifestEncoder, ManifestDecoder
from ..register import ICanonicalStorage, IStorableEntry, ICanonicalSource
from .readable import MemoizedReadable

logger = logging.getLogger(__name__)
logger.setLevel(int(os.environ.get('LOGLEVEL', '40')))

ET = timezone('US/Eastern')


class Filesystem(ICanonicalSource):
    """Retrieves content from a filesystem (outside the canonical record)."""

    def __init__(self, base_path: str) -> None:
        self._base_path = base_path

    def _make_path(self, uri: D.URI) -> str:
        return os.path.abspath(uri.path)

    def can_resolve(self, uri: D.URI) -> bool:
        return uri.is_file and self._base_path in os.path.abspath(uri.path)

    def load_deferred(self, uri: D.URI) -> IO[bytes]:
        """Make an IO that waits to load from the record until it is read()."""
        if not self.can_resolve(uri):
            raise RuntimeError(f'Cannot resolve this URI: {uri}')
        return open(self._make_path(uri), 'rb')


class CanonicalFilesystem(Filesystem, ICanonicalStorage):
    """Filesystem storage for the canonical record."""

    def can_resolve(self, uri: D.URI) -> bool:
        return uri.is_canonical

    def can_store(self, key: D.Key) -> bool:
        return key.is_canonical

    def _make_path(self, uri: D.URI) -> str:
        return os.path.join(self._base_path, uri.path.lstrip('/'))

    def list_subkeys(self, key: D.URI) -> List[str]:
        """List all of the subkeys for ``key`` in the record."""
        if not self.can_resolve(key):
            raise RuntimeError(f'Cannot resolve this URI: {key}')
        return os.listdir(self._make_path(key))

    def load_entry(self, uri: D.URI) -> Tuple[R.RecordStream, str]:
        """Load an entry from the record."""
        if not self.can_resolve(uri):
            raise RuntimeError(f'Cannot resolve this URI: {uri}')
        assert isinstance(uri, D.Key)
        path = self._make_path(uri)
        pointer = open(path, 'rb')
        content_type = D.ContentType.from_filename(path)
        size_bytes = os.stat(path).st_size
        stream = R.RecordStream(
            domain=D.CanonicalFile(
                modified=datetime.fromtimestamp(os.path.getmtime(path), tz=ET)
                    .astimezone(tz=UTC),
                filename=uri.filename,
                size_bytes=size_bytes,
                content_type=content_type,
                ref=uri
            ),
            content=pointer,
            content_type=content_type,
            size_bytes=size_bytes
        )
        return stream, I.calculate_checksum(pointer)

    def load_manifest(self, key: D.Key) -> Manifest:
        """Load an integrity manifest."""
        if not self.can_store(key):
            raise RuntimeError(f'Cannot load this manifest: {key}')
        with open(self._make_path(key), 'r') as f:
            manifest: Manifest = json.load(f, cls=ManifestDecoder)
        return manifest

    def store_entry(self, ri: IStorableEntry) -> None:
        """Store an entry in the record."""
        if not self.can_store(ri.record.key) or not ri.record.stream.content:
            logger.error(f'Cannot store: {ri.record.key}')
            raise RuntimeError(f'Cannot store: {ri.record.key}')

        path = self._make_path(ri.record.key)
        # Make sure that we have a place to put the file.
        parent, _ = os.path.split(path)
        if not os.path.exists(parent):
            os.makedirs(parent)

        # Ensure that we are starting from the beginning of the stream.
        logger.debug('Ready to write to %s from %s', path, ri.record.stream)
        if ri.record.stream.content.seekable():
            ri.record.stream.content.seek(0)

        # Write the content to the target file, being sure to decompress if
        # necessary.
        content: Union[IO[bytes], gzip.GzipFile]
        if ri.record.stream.domain.is_gzipped:
            content = gzip.GzipFile(fileobj=ri.record.stream.content)
        else:
            content = ri.record.stream.content
        with open(path, 'wb') as f:
            while True:
                chunk = content.read(4096)
                if not chunk:
                    break
                f.write(chunk)

        # Sanity check.
        size_bytes = os.path.getsize(path)
        logger.debug('Wrote %i bytes to %s', size_bytes, path)
        if size_bytes == 0:
            raise IOError(f'Wrote {size_bytes} bytes to {path}')

        # Update the CanonicalFile to reflect the fact that we decompressed
        # the content.
        if ri.record.stream.domain.is_gzipped:
            ri.record.stream.domain.is_gzipped = False
            ri.record.stream.domain.size_bytes = size_bytes
            ri.record.stream = ri.record.stream._replace(
                content=self.load_deferred(ri.record.key)
            )
            ri.update_checksum()

    def store_manifest(self, key: D.Key, manifest: Manifest) -> None:
        """Store an integrity manifest."""
        if not self.can_store(key):
            raise RuntimeError(f'Cannot store this manifest: {key}')
        path = self._make_path(key)
        parent, _ = os.path.split(path)
        if not os.path.exists(parent):
            os.makedirs(parent)   # Pave the way!
        with open(path, 'w') as f:
            f.write(json.dumps(manifest, cls=ManifestEncoder))
