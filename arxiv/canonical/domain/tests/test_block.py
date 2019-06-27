"""Tests for :class:`.MonthlyBlock`."""

from unittest import TestCase, mock
from datetime import date

from arxiv.taxonomy import Category
from ..eprint import EPrint
from ..block import MonthlyBlock
from ..identifier import VersionedIdentifier, Identifier


class TestIsOpen(TestCase):
    """Property :attr:`.is_open` indicates whether eprints can be added."""

    def test_last_month(self):
        """There is a block from a previous month."""
        block = MonthlyBlock(date.today().year, date.today().month - 1, {})
        self.assertFalse(block.is_open, 
                         'Only a block for the current month+year can be open')
        self.assertTrue(block.is_closed, 
                        'Only a block for the current month+year can be open')
    
    def test_last_year(self):
        """There is a block from last year."""
        block = MonthlyBlock(date.today().year - 1, date.today().month, {})
        self.assertFalse(block.is_open, 
                         'Only a block for the current month+year can be open')
        self.assertTrue(block.is_closed, 
                        'Only a block for the current month+year can be open')
    
    def test_this_month(self):
        """There is a block from this month."""
        block = MonthlyBlock(date.today().year, date.today().month, {})
        self.assertTrue(block.is_open, 
                        'Only a block for the current month+year can be open')
        self.assertFalse(block.is_closed, 
                         'Only a block for the current month+year can be open')


class TestGetNextIdentifier(TestCase):
    """The method :func:`.MonthlyBlock.get_next_identifer` gets identifiers."""

    def test_no_eprints(self):
        """There are no e-prints in the block."""
        year, month = 2043, 4
        block = MonthlyBlock(year, month, {})
        self.assertEqual(block.get_next_identifier(), '4304.00001',
                         'Returns the first identifier in the series')

    def test_some_eprints(self):
        """There are some e-prints in the block."""
        year, month = 2043, 4
        eprints = {
            VersionedIdentifier('4304.00001v4'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00005v1'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00001v2'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00001v3'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00004v1'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00003v1'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00001v1'): mock.MagicMock(spec=EPrint),
            VersionedIdentifier('4304.00002v1'): mock.MagicMock(spec=EPrint),
        }
        block = MonthlyBlock(year, month, eprints)
        self.assertEqual(block.get_next_identifier(), '4304.00006',
                         'Returns the first identifier in the series')
            


class TestAddEPrints(TestCase):
    """Add new eprints to the block."""

    def test_add_eprint_to_empty_block(self):
        """Add an eprint to an empty block."""
        year, month = date.today().year, date.today().month
        prefix = f'{str(year)[-2:]}{str(month).zfill(2)}'
        versioned_identifier = VersionedIdentifier(f'{prefix}.00001v4')
        eprint = mock.MagicMock(
            spec=EPrint,
            arxiv_id=Identifier(f'{prefix}.00001'),
            version=4,
            versioned_identifier=versioned_identifier
        )
        block = MonthlyBlock(year, month, {})
        block.add(eprint)
        self.assertIn(versioned_identifier, block.eprints,
                      'EPrint is added to the block')
        self.assertEqual(block.get_next_identifier(), f'{prefix}.00002',
                         'Next identifier comes after the added eprint')
    
    def test_add_duplicate_eprint(self):
        """Add the same eprint to the block twice."""
        year, month = date.today().year, date.today().month
        prefix = f'{str(year)[-2:]}{str(month).zfill(2)}'
        versioned_identifier = VersionedIdentifier(f'{prefix}.00001v4')
        eprint = mock.MagicMock(
            spec=EPrint,
            arxiv_id=Identifier(f'{prefix}.00001'),
            version=4,
            versioned_identifier=versioned_identifier,
        )
        block = MonthlyBlock(year, month, {})
        block.add(eprint)
        with self.assertRaises(ValueError):
            block.add(eprint)
    
    def test_add_eprint_to_wrong_block(self):
        """Add an eprint to an empty block."""
        year, month = date.today().year, date.today().month
        prefix = f'{str(year - 1)[-2:]}{str(month).zfill(2)}'
        versioned_identifier = VersionedIdentifier(f'{prefix}.00001v4')
        eprint = mock.MagicMock(
            spec=EPrint,
            arxiv_id=Identifier(f'{prefix}.00001'),
            version=4,
            versioned_identifier=versioned_identifier
        )
        block = MonthlyBlock(year, month, {})
        with self.assertRaises(ValueError):
            block.add(eprint)


class TestUpdateEPrints(TestCase):
    """Update existing eprints in the block."""

    def test_update_eprint_not_in_block(self):
        """Update an eprint that is not in the block."""
        year, month = date.today().year, date.today().month
        prefix = f'{str(year)[-2:]}{str(month).zfill(2)}'
        versioned_identifier = VersionedIdentifier(f'{prefix}.00001v4')
        eprint = mock.MagicMock(
            spec=EPrint,
            arxiv_id=Identifier(f'{prefix}.00001'),
            version=4,
            versioned_identifier=versioned_identifier
        )
        block = MonthlyBlock(year, month, {})
        with self.assertRaises(ValueError):
            block.update(eprint)
    
    def test_update_eprint(self):
        """Add the same eprint to the block twice."""
        year, month = date.today().year, date.today().month
        prefix = f'{str(year)[-2:]}{str(month).zfill(2)}'
        versioned_identifier = VersionedIdentifier(f'{prefix}.00001v4')
        eprint = mock.MagicMock(
            spec=EPrint,
            arxiv_id=Identifier(f'{prefix}.00001'),
            version=4,
            versioned_identifier=versioned_identifier,
            secondary_classification=[]
        )
        block = MonthlyBlock(year, month, {})
        block.add(eprint)
        eprint.secondary_classification.append(Category('foo.BR'))
        block.update(eprint)
        self.assertIn(
            Category('foo.BR'), 
            block.eprints[eprint.versioned_identifier].secondary_classification
        )
    