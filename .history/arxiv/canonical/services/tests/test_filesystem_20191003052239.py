"""Tests for :mod:`arxiv.canonical.services.filesystem`."""

import tempfile
from unittest import TestCase

from ..filesystem import Filesystem, D


class TestCanResolve(TestCase):
    """Filesystem service can resolve file URIs."""

    def setUp(self):
        """Given a filesystem."""
        self.base_path = tempfile.mkdtemp()
        self.filesystem = Filesystem(self.base_path)

    def test_with_http_uri(self):
        """Cannot resolve HTTP URIs."""
        self.assertFalse(self.filesystem.can_resolve(URI('https://asdf.com')))

    def test_with_canonical_uri(self):
        """Cannot resolve canonical URIs."""
        self.assertFalse(self.filesystem.can_resolve(URI('arxiv:///foo/key')))

    def test_with_file_uri(self):
        """CAN resolve file URIs."""
        self.assertTrue(self.filesystem.can_resolve(URI('file:///foo/key')))