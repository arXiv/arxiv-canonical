import io
import json
import os
import tempfile
from datetime import datetime
from pprint import pprint
from pytz import UTC
from typing import IO

from unittest import TestCase, mock

from ..version import IntegrityVersion, IntegrityEPrint, R, D


def fake_dereferencer(uri: D.URI) -> IO[bytes]:
    """Simulates a dereferencer for canonical URIs."""
    return io.BytesIO(b'fake content for ' + uri.encode('utf-8'))


class TestIntegrityVersion(TestCase):
    def setUp(self):
        """We have a RecordVersion..."""
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
                filename='2901.00345v1.tar.gz',
                created=created,
                modified=created,
                size_bytes=4_304,
                content_type=D.ContentType.targz,
                ref=D.URI('/fake/path.tar.gz')
            ),
            render=D.CanonicalFile(
                filename='2901.00345v1.pdf',
                created=created,
                modified=created,
                size_bytes=404,
                content_type=D.ContentType.pdf,
                ref=D.URI('/fake/path.pdf')
            )
        )
        self.record = R.RecordVersion.from_domain(self.version,
                                                  fake_dereferencer)

    def test_manifest(self):
        """IntegrityVersion makes a manifest from an IntegrityRecord."""
        integrity = IntegrityVersion.from_record(self.record)
        expected_entries = [
            {'key':
                'arxiv:///e-prints/2029/01/2901.00345/v1/2901.00345v1.json',
             'checksum': 'xLOiGxEmoytrXeB7Nw3lHw==',
             'size_bytes': 1187,
             'mime_type': 'application/json'},
            {'key': 'arxiv:///e-prints/2029/01/2901.00345/v1/2901.00345v1.pdf',
             'checksum': '7OdqCRhN09_flc5fVUZ1Tg==',
             'size_bytes': 404,
             'mime_type': 'application/pdf'},
            {'key':
                'arxiv:///e-prints/2029/01/2901.00345/v1/2901.00345v1.tar.gz',
             'checksum': '1GR0xuZYavi6N04v3-1wIw==',
             'size_bytes': 4304,
             'mime_type': 'application/gzip'}
        ]

        self.assertListEqual(integrity.manifest['entries'], expected_entries,
                             'Manifest contains the expected keys, checksums,'
                             ' sizes, and mime types.')
        self.assertEqual(integrity.manifest['number_of_versions'], 1,
                         'One version is included in the manifest')

    def test_checksum(self):
        """A checksum is calculated for the whole Version."""
        integrity = IntegrityVersion.from_record(self.record)
        self.assertEqual(integrity.checksum, 'Nodg72IZ_8yIBJ9p6Y5DcQ==',
                         'Generates the expected checksum for the Version')


class TestIntegrityEPrint(TestCase):
    def setUp(self):
        """We have a RecordEPrint..."""
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
                filename='2901.00345v1.tar.gz',
                created=created,
                modified=created,
                size_bytes=4_304,
                content_type=D.ContentType.targz,
                ref=D.URI('/fake/path.tar.gz')
            ),
            render=D.CanonicalFile(
                filename='2901.00345v1.pdf',
                created=created,
                modified=created,
                size_bytes=404,
                content_type=D.ContentType.pdf,
                ref=D.URI('/fake/path.pdf')
            )
        )
        self.eprint = D.EPrint(self.identifier.arxiv_id,
                               versions={self.identifier: self.version})
        self.record = R.RecordEPrint(
            self.identifier.arxiv_id,
            members={
                self.identifier:
                    R.RecordVersion.from_domain(self.version,
                                                fake_dereferencer)
            },
            domain=self.eprint
        )

    def test_checksum(self):
        """A checksum is calculated for the whole EPrint."""
        integrity = IntegrityEPrint.from_record(self.record)
        self.assertEqual(integrity.checksum, 'mWuHpIkY8mvFST0dmlgk4w==',
                         'Generates the expected checksum for the EPrint')

    def test_manifest(self):
        """A manifest is generated for the EPrint."""
        integrity = IntegrityEPrint.from_record(self.record)
        expected_entries = [
            {'key': '2901.00345v1',
             'checksum': 'Nodg72IZ_8yIBJ9p6Y5DcQ==',
             'number_of_versions': 1,
             'number_of_events': 0,
             'number_of_events_by_type': {}}
        ]
        self.assertListEqual(integrity.manifest['entries'], expected_entries)

    def test_members(self):
        integrity = IntegrityEPrint.from_record(self.record)
        self.assertEqual(integrity.members[self.identifier].record.domain,
                         self.version, 'Original Version is preserved')