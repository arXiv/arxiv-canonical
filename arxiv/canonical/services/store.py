"""
Persist changes to the canonical record.

Provides a :class:`.CanonicalStore` that stores resources in S3, using
:mod:`.serialize.record` to serialize and deserialize resources.
"""

import io
from base64 import urlsafe_b64decode, urlsafe_b64encode
from binascii import hexlify, unhexlify
from datetime import datetime, date
from functools import partial
from hashlib import md5
from json import dumps, load
from typing import Optional, Dict, Any, IO, List

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
from pytz import UTC

from .. import integrity
from ..domain import ContentType
from ..register import ICanonicalStorage
from ..serialize import record
from ..integrity import ManifestEntry, Manifest
from .readable import MemoizedReadable


class DoesNotExist(Exception):
    """The requested resource does not exist."""


class CanonicalStore(ICanonicalStorage):
    """
    Persists the canonical record in S3.

    The intended pattern for working with the canonical record is to use the
    :class:`.domain.CanonicalRecord` as the primary entrypoint for all
    operations. Consequently, this service offers only a single public instance
    method, :fund:`.load_record`.

    Persistence is achieved by attaching members to
    :class:`.domain.CanonicalRecord`, :class`.domain.Month`, and
    :class:`.domain.Listing` instances that implement reads/writes to S3. In
    this way, consumers of ``arxiv.canonical.domain`` can largely work directly
    with :class:`.domain.CanonicalRecord`, and persistence is handled
    transparently.
    """

    def __init__(self, bucket: str, verify: bool = False,
                 region_name: Optional[str] = None,
                 endpoint_url: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 read_only: bool = True) -> None:
        """Initialize with connection config parameters."""
        self._bucket = bucket
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._verify = verify
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._read_only = read_only
        self.client = self._new_client()

    @property
    def read_only(self) -> bool:
        """
        Determine whether or not this is a read-only session.

        This is read-only property to discourage users of this class to mess
        with it in runtime code. Should only be set via application
        configuration.
        """
        return self._read_only

    def inititalize(self) -> None:
        self.client.create_bucket(Bucket=self._bucket)

    def is_available(self, retries: int = 0, read_timeout: int = 5,
                     connect_timeout: int = 5) -> bool:
        """Determine whether or not we can read from/write to the store."""
        raise NotImplementedError('Implement me!')

    def load_entry(self, key: str) -> integrity.IntegrityEntry:
        response = self.client.get_object(Bucket=self._bucket, Key=key)
        return integrity.IntegrityEntry(
            record=record.RecordEntry(
                key=key,
                content=MemoizedReadable(response['Body'].read),
                content_type=ContentType.from_mimetype(response['ContentType']),
                size_bytes=response['ContentLength']
            ),
            checksum=_hex_to_b64(response['ETag'][1:-1])
        )

    def list_subkeys(self, key: str) -> List[str]:
        response = self.client.list_objects_v2(Bucket=self._bucket, Prefix=key)
        subs = [obj['Key'].split(key, 1)[1] for obj in response['Contents']]
        return [sub.split('/', 1)[0] if '/' in sub else sub for sub in subs]

    def store_entry(self, ri: integrity.IntegrityEntry) -> None:
        self.client.put_object(Bucket=self._bucket,
                               Key=ri.record.key,
                               Body=ri.record.content.read(),
                               ContentLength=ri.record.size_bytes,
                               ContentMD5=_b64_to_hex(ri.checksum),
                               ContentType=ri.record.content_type.mime_type)

    def store_manifest(self, key: str, manifest: Manifest) -> None:
        body = dumps(manifest).encode('utf-8')
        self.client.put_object(Bucket=self._bucket,
                               Key=key,
                               Body=body,
                               ContentLength=len(body),
                               ContentMD5=integrity.checksum_raw(body),
                               ContentType='application/json')

    def load_manifest(self, key: str) -> Manifest:
        response = self.client.get_object(Bucket=self._bucket, Key=key)
        manifest: Manifest = load(response['Body'])
        return manifest

    def _handle_client_error(self, exc: ClientError) -> None:
        if exc.response['Error']['Code'] == 'NoSuchBucket':
            raise DoesNotExist(f'{self._bucket} does not exist') from exc
        if exc.response['Error']['Code'] == "NoSuchKey":
            raise DoesNotExist(f'No such object in {self._bucket}') from exc
        raise RuntimeError('Unhandled ClientError') from exc

    def _new_client(self) -> boto3.client:
        # Only add credentials to the client if they are explicitly set.
        # If they are not set, boto3 falls back to environment variables and
        # credentials files.
        params: Dict[str, Any] = {'region_name': self._region_name}
        if self._aws_access_key_id and self._aws_secret_access_key:
            params['aws_access_key_id'] = self._aws_access_key_id
            params['aws_secret_access_key'] = self._aws_secret_access_key
        if self._endpoint_url:
            params['endpoint_url'] = self._endpoint_url
            params['verify'] = self._verify
        return boto3.client('s3', **params)


def _b64_to_hex(checksum: str) -> str:
    return hexlify(urlsafe_b64decode(checksum.encode('utf-8'))).decode('utf-8')


def _hex_to_b64(etag: str) -> str:
    """Convert an hexdigest of an MD5 to a URL-safe base64-encoded digest."""
    return urlsafe_b64encode(unhexlify(etag)).decode('utf-8')