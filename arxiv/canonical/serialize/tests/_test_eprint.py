"""Tests for :mod:`.serialize.base`."""

import io
import json
from unittest import TestCase
from datetime import date, datetime

from pytz import UTC

from arxiv.taxonomy import Category

from ....domain import EPrint, Identifier, File
from ...decoder import CanonicalDecoder
from ..base import checksum
from ..eprint import serialize, deserialize


class TestSerializeRecord(TestCase):
    """Test serialization of an eprint."""

    def setUp(self):
        """Instantiate and serialize an e-print."""
        source_content = io.BytesIO(b'fake targz content')
        source_checksum = checksum(source_content)
        self.source = File(
            filename='2004.00111.tar.gz',
            mime_type='application/gzip',
            checksum=source_checksum,
            content=source_content,
            created=datetime.now(UTC),
            modified=datetime.now(UTC)
        )

        pdf_content = io.BytesIO(b'fake pdf content')
        pdf_checksum = checksum(pdf_content)
        self.pdf = File(
            filename='2004.00111.pdf',
            mime_type='application/pdf',
            checksum=pdf_checksum,
            content=pdf_content,
            created=datetime.now(UTC),
            modified=datetime.now(UTC)
        )

        self.eprint = EPrint(
            arxiv_id=Identifier('2004.00111'),
            version=1,
            announced_date=date.today(),
            legacy=False,
            submitted_date=date.today(),
            license='http://notalicense',
            primary_classification=Category('cs.AR'),
            title='The Title of Everything',
            abstract='Very abstract. Too short to be a real abstract.',
            authors='Ima N. Author (FSU)',
            source_type='tex',
            size_kilobytes=1,
            previous_versions=[],
            secondary_classification=[Category('cs.AI'), Category('cs.DL')],
            history=[],
            source=self.source,
            pdf=self.pdf
        )

        self.serialized = serialize(self.eprint)

    def test_manifest_consistency(self):
        """Check internal consistency of the manifest."""
        manifest_data = json.load(self.serialized.manifest.content)
        self.assertEqual(manifest_data[self.serialized.metadata.key],
                         self.serialized.metadata.checksum,
                         'Manifest contains checksum of the metadata')
        self.assertEqual(manifest_data[self.serialized.source.key],
                         self.serialized.source.checksum,
                         'Manifest contains checksum of the source package')
        self.assertEqual(manifest_data[self.serialized.pdf.key],
                         self.serialized.pdf.checksum,
                         'Manifest contains checksum of the PDF')
    def test_metadata_consistency(self):
        """Check consistency of the metadata record."""
        metadata_data = json.load(self.serialized.metadata.content,
                                  cls=CanonicalDecoder)
        self.assertEqual(metadata_data.arxiv_id, self.eprint.arxiv_id)
        self.assertEqual(metadata_data.version, self.eprint.version)
        self.assertEqual(metadata_data.announced_date,
                         self.eprint.announced_date)
        self.assertEqual(metadata_data.submitted_date,
                         self.eprint.submitted_date)
        self.assertEqual(metadata_data.license, self.eprint.license)
        self.assertEqual(metadata_data.primary_classification,
                         self.eprint.primary_classification)
        self.assertEqual(metadata_data.secondary_classification,
                         self.eprint.secondary_classification)
        self.assertEqual(metadata_data.title, self.eprint.title)
        self.assertEqual(metadata_data.abstract, self.eprint.abstract)
        self.assertEqual(metadata_data.authors, self.eprint.authors)
        self.assertEqual(metadata_data.source_type, self.eprint.source_type)
        self.assertEqual(metadata_data.size_kilobytes,
                         self.eprint.size_kilobytes)


class TestDeserializeRecord(TestCase):
    """Test deserialization of an eprint record."""

    def setUp(self):
        """Instantiate and serialize an e-print."""
        source_content = io.BytesIO(b'fake targz content')
        source_checksum = checksum(source_content)
        self.source = File(
            filename='2004.00111.tar.gz',
            mime_type='application/gzip',
            checksum=source_checksum,
            content=source_content,
            created=datetime.now(UTC),
            modified=datetime.now(UTC)
        )

        pdf_content = io.BytesIO(b'fake pdf content')
        pdf_checksum = checksum(pdf_content)
        self.pdf = File(
            filename='2004.00111.pdf',
            mime_type='application/pdf',
            checksum=pdf_checksum,
            content=pdf_content,
            created=datetime.now(UTC),
            modified=datetime.now(UTC)
        )

        self.eprint = EPrint(
            arxiv_id=Identifier('2004.00111'),
            version=1,
            announced_date=date.today(),
            legacy=False,
            submitted_date=date.today(),
            license='http://notalicense',
            primary_classification=Category('cs.AR'),
            title='The Title of Everything',
            abstract='Very abstract. Too short to be a real abstract.',
            authors='Ima N. Author (FSU)',
            source_type='tex',
            size_kilobytes=1,
            previous_versions=[],
            secondary_classification=[Category('cs.AI'), Category('cs.DL')],
            history=[],
            source=self.source,
            pdf=self.pdf
        )

        self.serialized = serialize(self.eprint)

    def test_deserialize_metadata(self):
        """Check consistency of deserialized metadata."""
        eprint = deserialize(self.serialized)
        self.assertEqual(eprint.arxiv_id, self.eprint.arxiv_id)
        self.assertEqual(eprint.version, self.eprint.version)
        self.assertEqual(eprint.announced_date,
                         self.eprint.announced_date)
        self.assertEqual(eprint.submitted_date,
                         self.eprint.submitted_date)
        self.assertEqual(eprint.license, self.eprint.license)
        self.assertEqual(eprint.primary_classification,
                         self.eprint.primary_classification)
        self.assertEqual(eprint.secondary_classification,
                         self.eprint.secondary_classification)
        self.assertEqual(eprint.title, self.eprint.title)
        self.assertEqual(eprint.abstract, self.eprint.abstract)
        self.assertEqual(eprint.authors, self.eprint.authors)
        self.assertEqual(eprint.source_type, self.eprint.source_type)
        self.assertEqual(eprint.size_kilobytes,
                         self.eprint.size_kilobytes)

    def test_deserialize_content(self):
        """Check consistency of deserialized content."""
        eprint = deserialize(self.serialized)

        # The content of the original and deserialized objects may share the
        # same underlying IO object, so we should make sure that we are
        # starting from the beginning of the stream each time.
        eprint.pdf.content.seek(0)
        deserialized_content = eprint.pdf.content.read()
        self.eprint.pdf.content.seek(0)
        original_content = self.eprint.pdf.content.read()
        self.assertEqual(deserialized_content, original_content)

        eprint.source.content.seek(0)
        deserialized_content = eprint.source.content.read()
        self.eprint.source.content.seek(0)
        original_content = self.eprint.source.content.read()
        self.assertEqual(deserialized_content, original_content)


