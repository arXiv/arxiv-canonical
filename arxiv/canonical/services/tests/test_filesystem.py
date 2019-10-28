"""Tests for :mod:`arxiv.canonical.services.filesystem`."""

import io
import os
import tempfile
from unittest import TestCase

from ...domain import URI
from ..filesystem import Filesystem


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
        path = os.path.join(self.base_path, 'foo.json')
        self.assertTrue(self.filesystem.can_resolve(URI(path)))

    def test_with_file_uri_outside_base_path(self):
        """Cannot resolve file URIs that are outside of the base path."""
        self.assertFalse(self.filesystem.can_resolve(URI('file:///foo/key')))


class TestLoadDeferred(TestCase):
    """Filesystem service can load file URIs."""

    def setUp(self):
        """Given a file..."""
        self.base_path = tempfile.mkdtemp()
        self.filesystem = Filesystem(self.base_path)
        _, self.file_path = tempfile.mkstemp(dir=self.base_path)
        with open(self.file_path, 'wb') as f:
            f.write(b'some content')

    def test_load(self):
        """Can load content from the file."""
        resource = self.filesystem.load(URI(self.file_path))
        self.assertEqual(resource.read(4), b'some')

    def test_load_outside_base_path(self):
        """Cannot load a file outside of the base path"""
        _, other_path = tempfile.mkstemp()
        with self.assertRaises(RuntimeError):
            self.filesystem.load(URI(other_path))
