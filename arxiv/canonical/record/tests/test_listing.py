import io
import json
import os
import tempfile
from datetime import datetime
from pytz import UTC
from typing import IO
from unittest import TestCase, mock

import jsonschema

from ..core import RecordEntry
from ..listing import RecordListing
from ..version import RecordVersion, D


def fake_dereferencer(uri: D.URI) -> IO[bytes]:
    """Simulates a dereferencer for canonical URIs."""
    return io.BytesIO(b'fake content for ' + uri.encode('utf-8'))


class TestRecordListing(TestCase):
    """RecordListing provides keys and serialization for Listings."""

    SCHEMA_PATH = os.path.abspath('schema/resources/Listing.json')

    def setUp(self):
        """We have a Listing..."""
        with open(self.SCHEMA_PATH) as f:
            self.schema = json.load(f)

        self.resolver = jsonschema.RefResolver(
            'file://%s/' % os.path.abspath(os.path.dirname(self.SCHEMA_PATH)),
            None
        )

        self.identifier = D.VersionedIdentifier('2901.00345v1')
        self.created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
        self.listing_id = D.ListingIdentifier.from_parts(self.created.date(),
                                                         'foo')

        self.version = D.Version(
            identifier=self.identifier,
            announced_date=self.created.date(),
            announced_date_first=self.created.date(),
            submitted_date=self.created,
            updated_date=self.created,
            is_announced=True,
            events=[],
            previous_versions=[],
            metadata=D.Metadata(
                primary_classification=D.Category('cs.DL'),
                secondary_classification=[D.Category('cs.IR')],
                title='Foo title',
                abstract='It is abstract',
                authors='Ima N. Author (FSU)',
                license=D.License(href="http://some.license")
            ),
            source=D.CanonicalFile(
                filename='2901.00345v1.tar.gz',
                created=self.created,
                modified=self.created,
                size_bytes=4_304,
                content_type=D.ContentType.targz,
                ref=D.URI('/fake/path.tar.gz')
            ),
            render=D.CanonicalFile(
                filename='2901.00345v1.pdf',
                created=self.created,
                modified=self.created,
                size_bytes=404,
                content_type=D.ContentType.pdf,
                ref=D.URI('/fake/path.pdf')
            )
        )
        self.event = D.Event(
            identifier=self.identifier,
            event_date=self.created,
            event_type=D.EventType.NEW,
            categories=[D.Category('cs.DL')],
            version=self.version
        )
        self.listing = D.Listing(self.listing_id, [self.event])

    def test_from_domain(self):
        """Can load a RecordListing from a Listing domain object."""
        record = RecordListing.from_domain(self.listing)
        self.assertEqual(record.created, self.created)
        self.assertEqual(
            record.key,
            'arxiv:///announcement/2029/01/29/2029-01-29-foo.json',
            'Key for listing file is generated correctly'
        )
        self.assertEqual(record.key, record.stream.domain.ref)
        self.assertEqual(record.stream.content_type, D.ContentType.json,
                         'Correctly identified as a JSON resource')
        raw = json.load(record.stream.content)
        # TODO: update schema so that this can pass!
        jsonschema.validate(raw, self.schema, resolver=self.resolver)
