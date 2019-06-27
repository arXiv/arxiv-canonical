"""Tests for :mod:`.domain.record`."""

from unittest import TestCase, mock
from datetime import date

from arxiv.taxonomy import Category
from ..eprint import EPrint
from ..block import MonthlyBlock
from ..identifier import VersionedIdentifier, Identifier
from ..record import CanonicalRecord
from ..listing import Listing
from ..event import Event


class TestCurrentBlock(TestCase):
    """Property :attr:`.current_block` is the current :class:`.MonthlyBlock."""

    def test_current_block(self):
        """There are several blocks on the record."""
        year, month = date.today().year, date.today().month
        blocks = {
            (year - 1, month): MonthlyBlock(year - 1, month, {}),
            (year, month): MonthlyBlock(year, month, {}),
            (year, month + 1): MonthlyBlock(year, month + 1, {}),
        }
        record = CanonicalRecord(blocks, {})
        self.assertIsInstance(record.current_block, MonthlyBlock)
        self.assertEqual(record.current_block.year, year)
        self.assertEqual(record.current_block.month, month)
        self.assertTrue(record.current_block.is_open)


class TestAnnounceNew(TestCase):
    """We have new e-prints that require announcement."""

    def test_unannounced_eprint(self):
        """We have an e-print that is not announced."""
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
            history=[]
        )
        self.assertFalse(eprint.is_announced, 'E-Print is not announced')

        year, month = date.today().year, date.today().month
        blocks = {
            (year - 1, month): MonthlyBlock(year - 1, month, {}),
            (year, month): MonthlyBlock(year, month, {}),
            (year, month + 1): MonthlyBlock(year, month + 1, {}),
        }
        today_listing = Listing(date.today(), [])
        listings = {(date.today()): today_listing}
        record = CanonicalRecord(blocks, listings)
        eprint = record.announce_new(eprint)

        self.assertTrue(eprint.is_announced, 'E-Print is announced')
        self.assertIn(eprint.versioned_identifier, 
                      record.current_block.eprints,
                      'E-Print is in the current block')
        self.assertEqual(len(today_listing.events), 1, 
                         'An event is added to the listing')
        self.assertIsInstance(today_listing.events[0], Event)
        self.assertEqual(today_listing.events[0].arxiv_id, eprint.arxiv_id,
                         'Event has the correct arXiv ID')
        self.assertEqual(today_listing.events[0].version, eprint.version,
                         'Event has the correct version')

    def test_previously_announced_eprint(self):
        """We have an e-print that is already announced."""
        eprint = EPrint(
            arxiv_id='1702.00123',
            version=2,
            announced_date=date(year=2017, month=2, day=5),
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
            history=[]
        )
        self.assertTrue(eprint.is_announced, 'E-Print is not announced')

        year, month = date.today().year, date.today().month
        blocks = {
            (year - 1, month): MonthlyBlock(year - 1, month, {}),
            (year, month): MonthlyBlock(year, month, {}),
            (year, month + 1): MonthlyBlock(year, month + 1, {}),
        }
        listings = {(date.today()): Listing(date.today(), [])}
        record = CanonicalRecord(blocks, listings)

        with self.assertRaises(ValueError):
            record.announce_new(eprint)
        
        