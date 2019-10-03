import io
from base64 import urlsafe_b64encode
from hashlib import md5
from operator import itemgetter
from typing import List, IO, Union, cast

from ..record import RecordStream
from .manifest import Manifest
from .exceptions import ChecksumError


def calculate_checksum(obj: Union[bytes, IO[bytes], Manifest, RecordStream]) \
        -> str:
    if isinstance(obj, bytes):
        return checksum_raw(obj)
    if isinstance(obj, dict):
        return checksum_manifest(cast(Manifest, obj))
    if isinstance(obj, io.IOBase):
        return checksum_io(obj)
    if isinstance(obj, RecordStream):
        print(obj)
        assert obj.content is not None
        return checksum_io(obj.content)
    raise TypeError(f'Cannot generate a checksum from a {type(obj)}')


def checksum_raw(raw: bytes) -> str:
    hash_md5 = md5()
    hash_md5.update(raw)
    return urlsafe_b64encode(hash_md5.digest()).decode('utf-8')


def checksum_io(content: IO[bytes]) -> str:
    """Generate an URL-safe base64-encoded md5 hash of an IO."""
    if content.seekable:
        content.seek(0)     # Make sure that we are at the start of the stream.
    hash_md5 = md5()
    for chunk in iter(lambda: content.read(4096), b""):
        hash_md5.update(chunk)
    if content.seekable:
        content.seek(0)     # Be a good neighbor for subsequent users.
    return urlsafe_b64encode(hash_md5.digest()).decode('utf-8')


def checksum_manifest(manifest: Manifest) -> str:
    components: List[str] = []
    for entry in sorted(manifest['entries'], key=itemgetter('key')):
        if 'checksum' not in entry or entry['checksum'] is None:
            raise ChecksumError(f'Missing checksum: {entry}')
        components.append(entry['checksum'])
    return checksum_raw(''.join(components).encode('utf-8'))