"""
Persist changes to the canonical record.

Provides a :class:`.CanonicalStore` that stores :class:`.Listing` and 
:class:`.EPrint` resources in S3.
"""

import io
from typing import Optional, Dict, Any
from datetime import datetime, date
from pytz import UTC

from flask import Flask
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

from arxiv.base.globals import get_application_global, get_application_config
from arxiv.taxonomy import Category

from ..domain import Listing, EPrint, Identifier, Event, License, File, Person


class DoesNotExist(Exception):
    """The requested resource does not exist."""


# TODO: implement me!
class PersistentListing(Listing):
    """
    A :class:`.Listing` with persistent features.
    
    The objective of this class is to keep track of what has changed and 
    requires storing in a request/operation context.
    """

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
    
    def load_listing(self, start_date: date, end_date: Optional[date] = None) \
            -> PersistentListing:
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


class FakePersistentListing(PersistentListing):
    @property
    def is_changed(self) -> bool:
        return False


class FakePersistentEPrint(PersistentEPrint):
    @property
    def is_changed(self) -> bool:
        return False


class FakeCanonicalStore(CanonicalStore):
    """
    A mock implementation of the canonical store.
    
    Methods to store things don't do anything, so don't expect data to stick
    around.
    """

    @classmethod
    def current_session(cls) -> 'FakeCanonicalStore':
        return cls('foo')

    def store_listing(self, listing: PersistentListing) -> None:
        return
    
    def store_eprint(self, eprint: EPrint) -> None:
        return

    def load_listing(self, start_date: date, end_date: Optional[date] = None) \
            -> PersistentListing:
        return FakePersistentListing(
            start_date=start_date,
            end_date=start_date,
            events=[
                Event(arxiv_id=Identifier('2004.00321'),
                      event_date=datetime.now(UTC),
                      event_type=Event.Type.NEW,
                      categories=[Category('cs.DL'), Category('cs.AI')],
                      version=1),
                Event(arxiv_id=Identifier('2004.00322'),
                      event_date=datetime.now(UTC),
                      event_type=Event.Type.NEW,
                      categories=[Category('cs.DL'), Category('cs.AI')],
                      version=1),
                Event(arxiv_id=Identifier('2003.00021'),
                      event_date=datetime.now(UTC),
                      event_type=Event.Type.CROSSLIST,
                      categories=[Category('cs.AR')],
                      version=1),
                Event(arxiv_id=Identifier('2003.00001'),
                      event_date=datetime.now(UTC),
                      event_type=Event.Type.REPLACED,
                      categories=[Category('cs.AR')],
                      version=2)
            ]
        )

    def load_eprint(self, identifier: Identifier, version: int) \
            -> PersistentEPrint:
        return FakePersistentEPrint(
            arxiv_id=identifier,
            announced_date=date.today(),
            version=1,
            legacy=True,
            submitted_date=datetime.now(UTC),
            license=License(
                href="https://arxiv.org/licenses/nonexclusive-distrib/1.0/"
                     "license.html"
            ),
            primary_classification=Category("cs.DL"),
            title="Adventures in Flatland",
            abstract="As Gregor Samsa awoke one morning from uneasy dreams he"
                     " found himself transformed in his bed into a gigantic"
                     " insect. He was lying on his hard, as it were"
                     " armor-plated, back and when he lifted his head a little"
                     " he could see his dome-like brown belly divided into"
                     " stiff arched segments on top of which the bed quilt"
                     " could hardly keep in position and was about to slide"
                     " off completely. His numerous legs, which were pitifully"
                     " thin compared to the rest of his bulk, waved helplessly"
                     " before his eyes.",
            authors="Ima N. Author (FSU)",
            source_type="tex",
            size_kilobytes=543,
            previous_versions=[],
            secondary_classification=[Category('cs.AI'), Category('cs.AR')],
            history=[
                Event(arxiv_id=identifier,
                      event_date=datetime.now(UTC),
                      event_type=Event.Type.NEW,
                      categories=[Category('cs.DL'),
                                  Category('cs.AI'),
                                  Category('cs.AR')],
                      version=1),
            ],
            submitter=Person(
                full_name="Ima N. Author",
                last_name="Author",
                first_name="Ima N.",
                affiliation=["FSU"]
            ),
            comments="4 figures, 2 turtles",
            source_package=File(
                filename=f"{identifier}.tar.gz",
                mime_type="application/tar+gzip",
                checksum="asdf1234==",
                content=io.BytesIO(b'foocontent'),
                created=datetime.now(UTC),
                modified=datetime.now(UTC)
            ),
            pdf=File(
                filename=f"{identifier}.pdf",
                mime_type="application/pdf",
                checksum="qwer9876==",
                content=io.BytesIO(b'foopdf'),
                created=datetime.now(UTC),
                modified=datetime.now(UTC)
            )
        )
        

