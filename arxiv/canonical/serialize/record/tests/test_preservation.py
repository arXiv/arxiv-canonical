"""Tests for :mod:`.serialize.record.preservation`."""

import io
import json
from unittest import TestCase
from datetime import date, datetime

from pytz import UTC

from arxiv.taxonomy import Category

from ....domain import EPrint, Identifier, File, MonthlyBlock, \
    CanonicalRecord, Listing
from ...decoder import CanonicalJSONDecoder
from ..base import checksum
from ..preservation import serialize


class TestSerializePreservationRecord(TestCase):
    def setUp(self):
        """Create a :class:`.CanonicalRecord`."""
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

        eprint = EPrint(
            arxiv_id=None,
            version=None,
            announced_date=date.today(),
            legacy=False,
            submitted_date=date.today(),
            license='http://notalicense',
            primary_classification='cs.AR',
            title='The Title of Everything',
            abstract='Very abstract. Too short to be a real abstract.',
            authors='Ima N. Author (FSU)',
            source_type='tex',
            size_kilobytes=1,
            previous_versions=[],
            secondary_classification=['cs.AI', 'cs.DL'],
            history=[],
            source_package=self.source,
            pdf=self.pdf
        )

        year, month = date.today().year, date.today().month
        blocks = {
            (year - 1, month): MonthlyBlock(year - 1, month, {}),
            (year, month): MonthlyBlock(year, month, {}),
            (year, month + 1): MonthlyBlock(year, month + 1, {}),
        }
        today_listing = Listing(date.today(), [])
        listings = {(date.today()): today_listing}
        self.record = CanonicalRecord(blocks, listings)
        eprint = self.record.announce_new(eprint)

    def test_foo(self):
        print(serialize(self.record, date.today()))

        for key, entry in serialize(self.record, date.today()):
            print(key, entry.checksum)