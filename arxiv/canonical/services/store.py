"""
Persist changes to the canonical record.

Provides a :class:`.CanonicalStore` that stores :class:`.Listing` and 
:class:`.EPrint` resources in S3.
"""

from typing import Optional, Dict, Any
from datetime import datetime

from flask import Flask
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

from arxiv.base.globals import get_application_global, get_application_config

from ..domain import Listing, EPrint, Identifier


class DoesNotExist(Exception):
    """The requested resource does not exist."""


# TODO: implement me!
class PersistentListing(Listing):
    """
    A :class:`.Listing` with persistent features.
    
    The objective of this class is to keep track of what has changed and 
    requires storing in a request/operation context.
    """

    def __init__(self, *args, **kwargs) -> None:
        """Set up private structs for tracking what has changed."""
        raise NotImplementedError('Implement me!')
    
    @property
    def is_changed(self):
        """Indicate whether or not this listing has unpersisted changes."""
        raise NotImplementedError('Implement me!')


# TODO: implement me!
class PersistentEPrint(EPrint):
    """
    A :class:`.EPrint` with persistent features.
    
    The objective of this class is to keep track of what has changed and 
    requires storing in a request/operation context.
    """

    def __init__(self, *args, **kwargs) -> None:
        """Set up private structs for tracking what has changed."""
        raise NotImplementedError('Implement me!')
    
    @property
    def is_changed(self):
        """Indicate whether or not this e-print has unpersisted changes."""
        raise NotImplementedError('Implement me!')


# TODO: implement me!
class CanonicalStore:
    """Persists the canonical record in S3."""

    def __init__(self, bucket: str, verify: bool = False,
                 region_name: Optional[str] = None,
                 endpoint_url: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None) -> None:
        """Initialize with connection config parameters."""
        self._bucket = bucket
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._verify = verify
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self.client = self._new_client()

    def _new_client(self, config: Optional[Config] = None) -> boto3.client:
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

    def _handle_client_error(self, exc: ClientError) -> None:
        if exc.response['Error']['Code'] == 'NoSuchBucket':
            raise DoesNotExist(f'{self._bucket} does not exist') from exc
        if exc.response['Error']['Code'] == "NoSuchKey":
            raise DoesNotExist(f'No such object in {self._bucket}') from exc
        raise RuntimeError('Unhandled ClientError') from exc
    
    def is_available(self, retries: int = 0, read_timeout: int = 5,
                     connect_timeout: int = 5) -> bool:
        """Determine whether or not we can read from/write to the store."""
        raise NotImplementedError('Implement me!')

    def store_listing(self, listing: PersistentListing) -> None:
        """
        Store a :class:`.Listing`.
        """
        raise NotImplementedError('Implement me!')
    
    def store_eprint(self, eprint: PersistentEPrint) -> None:
        """
        Store a :class:`.EPrint`.

        If the :attr:`.EPrint.source_package` or :attr:`.EPrint.pdf` content
        has changed, those should also be stored.
        """
        raise NotImplementedError('Implement me!')
    
    def load_listing(self, start_date: datetime, 
                     end_date: Optional[datetime] = None) -> PersistentListing:
        """
        Load a listing, and all of its attendant events and e-prints.
        
        Parameters
        ----------
        start_date : datetime
            Start date for selecting listing events.
        end_date : datetime or None
            End date for selecting listing events (inclusive). If ``None``,
            should default to ``start_date`` (load listings for a single day).

        Returns
        -------
        :class:`.PersistentListing`

        """
        raise NotImplementedError('Implement me!')

    def load_eprint(self, identifier: Identifier, version: int) \
            -> PersistentEPrint:
        """
        Load an :class:`.EPrint`.

        The content of the :attr:`.EPrint.source_package` and 
        :attr:`.EPrint.pdf` should implement :class:`.Readable`. The ``read()``
        method should be a closure that, when called, retrieves the content of 
        the corresponding resource from storage.
        """
        raise NotImplementedError('Implement me!')
    
    @classmethod
    def init_app(cls, app: Flask) -> None:
        """Set defaults for required configuration parameters."""
        app.config.setdefault('AWS_REGION', 'us-east-1')
        app.config.setdefault('AWS_ACCESS_KEY_ID', None)
        app.config.setdefault('AWS_SECRET_ACCESS_KEY', None)
        app.config.setdefault('S3_ENDPOINT', None)
        app.config.setdefault('S3_VERIFY', True)
        app.config.setdefault('S3_BUCKET', 'arxiv-canonical')

    @classmethod
    def get_session(cls) -> 'CanonicalStore':
        """Create a new :class:`botocore.client.S3` session."""
        config = get_application_config()
        return cls(config['S3_BUCKET'],
                   config['S3_VERIFY'],
                   config['AWS_REGION'],
                   config['S3_ENDPOINT'],
                   config['AWS_ACCESS_KEY_ID'],
                   config['AWS_SECRET_ACCESS_KEY'])

    @classmethod
    def current_session(cls) -> 'CanonicalStore':
        """Get the current store session for this application."""
        g = get_application_global()
        if g is None:
            return cls.get_session()
        if 'store' not in g:
            g.store = cls.get_session()
        store: CanonicalStore = g.store
        return store