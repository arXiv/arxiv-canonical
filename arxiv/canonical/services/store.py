"""
Persist changes to the canonical record.

Provides a :class:`.CanonicalStore` that stores resources in S3, using
:mod:`.serialize.record` to serialize and deserialize resources.
"""

import io
from unittest import mock    # TODO: remove this when fakes are no longer used.
from typing import Optional, Dict, Any, IO
from datetime import datetime, date
from pytz import UTC
from functools import partial

from flask import Flask
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

from arxiv.base.globals import get_application_global, get_application_config
from arxiv.taxonomy import Category

from ..domain import Listing, EPrint, Identifier, Event, License, File, \
    Person, CanonicalRecord, MonthlyBlock
from ..serialize.record import eprint
from .readable import MemoizedReadable


class DoesNotExist(Exception):
    """The requested resource does not exist."""


# TODO: implement me!
class CanonicalStore:
    """
    Persists the canonical record in S3.

    The intended pattern for working with the canonical record is to use the
    :class:`.domain.CanonicalRecord` as the primary entrypoint for all
    operations. Consequently, this service offers only a single public instance
    method, :fund:`.load_record`.

    Persistence is achieved by attaching members to
    :class:`.domain.CanonicalRecord`, :class`.domain.MonthlyBlock`, and
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

    def load_record(self) -> CanonicalRecord:
        """
        Initialize and return the :class:`.CanonicalRecord`.

        The ``blocks`` and ``listings`` members must be mappings that implement
        ``__getitem__`` methods such that, when called, an object of the
        expected type (:class:`.MonthlyBlock` and :class:`.Listing`,
        respectively) is always returned.
        """
        raise NotImplementedError('Implement me!')

    def _load_listing(self, listing_date: date) -> Listing:
        """
        Load a :class:`.Listing`.

        If ``self.read_only`` is ``False``, the ``events`` member of the
        listing must be a subclass of ``list``, and implement an
        ``append(event: Event) -> None`` method that, when called, writes the
        current state of the listing to S3.

        Parameters
        ----------
        listing_date : datetime
            Date for selecting listing events.

        Returns
        -------
        :class:`.Listing`

        """
        raise NotImplementedError('Implement me!')

    def _load_block(self, year: int, month: int) -> MonthlyBlock:
        """
        Load a :class:`.MonthlyBlock`.

        The ``eprints`` member of the block must be a mapping (e.g. subclass of
        ``dict``), and implement:

        - If ``self.read_only`` is ``False``, a method
          ``__setitem__(identifier: VersionedIdentifier, eprint: EPrint) ->
          None`` that, when called, writes the :class:`.EPrint` to S3.
        - A method ``__getitem__(identifier: VersionedIdentifier) -> EPrint:``
          that, when called, reads the corresponding :class:`.EPrint` from
          S3 if it exists (otherwise raises ``KeyError``).

        Parameters
        ----------
        year : int
        month : int

        Returns
        -------
        :class:`.MonthlyBlock`

        """
        raise NotImplementedError('Implement me!')

    def _store_listing(self, listing: Listing) -> None:
        """
        Store a :class:`.Listing`.

        Should complain loudly if ``self.read_only`` is ``True``.
        """
        raise NotImplementedError('Implement me!')

    def _store_eprint(self, eprint: EPrint) -> None:
        """
        Store an :class:`.EPrint`.

        If the :attr:`.EPrint.source_package` or :attr:`.EPrint.pdf` content
        has changed, those should also be stored.

        Should complain loudly if ``self.read_only`` is ``True``.
        """
        raise NotImplementedError('Implement me!')

    def _load_eprint(self, arxiv_id: Identifier, version: int) \
            -> EPrint:
        """
        Load an :class:`.EPrint`.

        The content of the :attr:`.EPrint.source_package` and
        :attr:`.EPrint.pdf.content` should provide a ``read()`` method that,
        when called, retrieves the content of the corresponding resource from
        storage.
        """
        prefix = eprint.EPrintRecord.key_prefix(arxiv_id.year, arxiv_id.month,
                                                str(arxiv_id), version)
        metadata_key = eprint.MetadataEntry.make_key(prefix, arxiv_id, version)
        pdf_key = eprint.PDFEntry.make_key(prefix, arxiv_id, version)
        source_key = eprint.SourceEntry.make_key(prefix, arxiv_id, version)
        manifest_key = eprint.ManifestEntry.make_key(prefix, arxiv_id, version)

        record = eprint.EPrintRecord(
            metadata=eprint.MetadataEntry(
                key=metadata_key,
                content=MemoizedReadable(partial(self._load_key, metadata_key))
            ),
            source=eprint.SourceEntry(
                key=source_key,
                content=MemoizedReadable(partial(self._load_key, source_key))
            ),
            pdf=eprint.PDFEntry(
                key=pdf_key,
                content=MemoizedReadable(partial(self._load_key, pdf_key))
            ),
            manifest=eprint.ManifestEntry(
                key=manifest_key,
                content=MemoizedReadable(partial(self._load_key, manifest_key))
            )
        )
        return eprint.deserialize(record)

    def _load_key(self, key: str) -> bytes:
        return b'{"fake": "data"}'

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


class FakeCanonicalStore(CanonicalStore):
    """
    A mock implementation of the canonical store.

    Methods to store things don't do anything, so don't expect data to stick
    around.
    """

    @classmethod
    def current_session(cls) -> 'FakeCanonicalStore':
        return cls('foo')

    def store_listing(self, listing: Listing) -> None:
        return

    def store_eprint(self, eprint: EPrint) -> None:
        return

    def load_record(self) -> CanonicalRecord:
        fake_eprints = mock.MagicMock(spec=dict)
        identifier = Identifier('1901.00123')
        fake_eprint = EPrint(
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
        fake_eprints.__getitem__.return_value = fake_eprint
        fake_block = mock.MagicMock(spec=MonthlyBlock,
                                    eprints=fake_eprints)
        fake_block.load_eprint.return_value = fake_eprint
        fake_blocks = mock.MagicMock(spec=dict)
        fake_blocks.__getitem__.return_value = fake_block

        fake_listings = mock.MagicMock(spec=dict)
        fake_listings.__getitem__.return_value = Listing(
            date=date.today(),
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
        return CanonicalRecord(blocks=fake_blocks, listings=fake_listings)
