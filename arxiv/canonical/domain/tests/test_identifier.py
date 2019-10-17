"""Tests for :class:`.Identifier`."""

from unittest import TestCase

from .. import Identifier, VersionedIdentifier


class TestIdentifierComparisons(TestCase):
    """Test comparisons between identifiers."""

    def test_compare_oldstyle_identifiers(self):
        self.assertLess(Identifier('hep-ex/9802024'),
                        Identifier('cond-mat/9805021'))
        self.assertLessEqual(Identifier('hep-ex/9802024'),
                             Identifier('cond-mat/9805021'))
        self.assertGreater(Identifier('cond-mat/9805021'),
                           Identifier('hep-ex/9802024'))
        self.assertGreaterEqual(Identifier('cond-mat/9805021'),
                                Identifier('hep-ex/9802024'))