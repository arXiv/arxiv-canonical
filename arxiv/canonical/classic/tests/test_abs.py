"""Tests for :mod:`arxiv.canonical.classic.abs`."""

import os
from unittest import TestCase, mock

from ...domain import EventType
from .. import abs


class TestParseWithdrawn(TestCase):
    """Parse abs file for a withdrawn e-print."""
    DATA = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

    def test_withdrawn(self):
        """Parsed data should indicate withdrawn submission."""
        data = abs._parse(os.path.join(self.DATA, 'withdrawn.abs'))
        self.assertEqual(data.submission_type, EventType.WITHDRAWN)