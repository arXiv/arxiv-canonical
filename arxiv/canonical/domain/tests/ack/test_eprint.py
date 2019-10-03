"""Tests for :mod:`.eprint."""

from unittest import TestCase
from datetime import date

from arxiv.taxonomy import Category

from ..eprint import EPrint


class TestCategories(TestCase):
    """Test handling of categories on e-prints."""

    def setUp(self):
        """Instantiate an e-print."""
        self.eprint = EPrint(
            arxiv_id='2004.00111',
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
            history=[]
        )
    
    def test_all_categories(self):
        """Get all categories on the e-print."""
        self.assertIn(self.eprint.primary_classification, 
                      self.eprint.all_categories,
                      'The primary category is included')
        for category in self.eprint.secondary_classification:
            self.assertIn(category, self.eprint.all_categories,
                          'All secondary categories are included')
        self.assertEqual(self.eprint.primary_classification, 
                         self.eprint.all_categories[0],
                         'The primary category comes first')
    
    def test_add_secondaries(self):
        """Add secondary categories to an e-print."""
        self.eprint.add_secondaries(Category('foo.CT'), Category('ww.JD'))
        self.assertIn('foo.CT', self.eprint.secondary_classification)
        self.assertIn('ww.JD', self.eprint.secondary_classification)

    
class TestVersionedIdentifier(TestCase):
    """Test :attr:`EPrint.versioned_identifier` property."""

    def test_with_id_and_version_set(self):
        """Get the versioned identifier for an announced eprint."""
        eprint = EPrint(
            arxiv_id='2004.00111',
            version=5,
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
        self.assertEqual(eprint.versioned_identifier, '2004.00111v5',
                         'The versioned identifier is a concatenation of the'
                         'arXiv ID and the ersion number.')
    
    def test_without_version(self):
        """Get the versioned identifier when version is missing"""
        eprint = EPrint(
            arxiv_id='2004.00111',
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
        with self.assertRaises(ValueError):
            eprint.versioned_identifier
        
    def test_without_arxiv_id(self):
        """Get the versioned identifier when arxiv_id is missing"""
        eprint = EPrint(
            arxiv_id=None,
            version=1,
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
        with self.assertRaises(ValueError):
            eprint.versioned_identifier