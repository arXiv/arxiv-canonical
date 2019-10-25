
import io
import json
import os
import tempfile
from datetime import datetime
from pprint import pprint
from pytz import UTC
from typing import IO
from unittest import TestCase, mock

import jsonschema

from ..core import RecordEntry
from ..metadata import RecordMetadata
from ..version import RecordVersion, D


def fake_dereferencer(uri: D.URI) -> IO[bytes]:
    """Simulates a dereferencer for canonical URIs."""
    return io.BytesIO(b'fake content for ' + uri.encode('utf-8'))


class TestRecordVersion(TestCase):
    """RecordVersion provides keys and serialization for Versions."""

    SCHEMA_PATH = os.path.abspath('schema/resources/Version.json')

    def setUp(self):
        """We have a Version..."""
        with open(self.SCHEMA_PATH) as f:
            self.schema = json.load(f)

        self.resolver = jsonschema.RefResolver(
            'file://%s/' % os.path.abspath(os.path.dirname(self.SCHEMA_PATH)),
            None
        )

        self.identifier = D.VersionedIdentifier('2901.00345v1')
        created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
        self.version = D.Version(
            identifier=self.identifier,
            announced_date=created.date(),
            announced_date_first=created.date(),
            submitted_date=created,
            updated_date=created,
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
                filename='2901.00345v1.tar',
                modified=created,
                size_bytes=4_304,
                content_type=D.ContentType.tar,
                ref=D.URI('/fake/path.tar.gz'),
                is_gzipped=True,
            ),
            render=D.CanonicalFile(
                filename='2901.00345v1.pdf',
                modified=created,
                size_bytes=404,
                content_type=D.ContentType.pdf,
                ref=D.URI('/fake/path.pdf')
            )
        )

    def test_identifier(self):
        """RecordVersion exposes the identifier of the domain object."""
        record = RecordVersion.from_domain(self.version, fake_dereferencer)
        self.assertEqual(record.identifier, self.identifier,
                         'Version identifier is accessible')

    def test_from_domain(self):
        """Can load a RecordVersion from a Version domain object."""
        record = RecordVersion.from_domain(self.version, fake_dereferencer)
        self.assertTrue(record.metadata.key.is_canonical)
        self.assertEqual(
            record.metadata.key,
            'arxiv:///e-prints/2029/01/2901.00345/v1/2901.00345v1.json',
            'Key for metadadata record is generated correctly'
        )

        self.assertTrue(record.render.key.is_canonical)
        self.assertEqual(
            record.render.key,
            'arxiv:///e-prints/2029/01/2901.00345/v1/2901.00345v1.pdf',
            'Key for render is generated correctly'
        )
        self.assertEqual(record.render.stream.content.read(),
                         b'fake content for file:///fake/path.pdf',
                         'Render resource is dereferenced correctly')

        self.assertTrue(record.source.key.is_canonical)
        self.assertEqual(
            record.source.key,
            'arxiv:///e-prints/2029/01/2901.00345/v1/2901.00345v1.tar.gz',
            'Key for source package is generated correctly'
        )
        self.assertEqual(record.source.stream.content.read(),
                         b'fake content for file:///fake/path.tar.gz',
                         'Source resource is dereferenced correctly')

    def test_schema(self):
        """Serialized record is schema compliant."""
        record = RecordVersion.from_domain(self.version, fake_dereferencer)
        raw = json.load(record.metadata.stream.content)
        jsonschema.validate(raw, self.schema, resolver=self.resolver)

    def test_to_domain(self):
        """Re-casting to domain should preserve state."""
        record = RecordVersion.from_domain(self.version, fake_dereferencer)
        cast_version = record.instance_to_domain()
        for key in self.version.__dict__.keys():
            self.assertEqual(getattr(cast_version, key),
                             getattr(self.version, key),
                             f'{key} should match')
