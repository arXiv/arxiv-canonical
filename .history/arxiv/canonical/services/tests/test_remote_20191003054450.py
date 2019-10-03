"""Tests for :mod:`arxiv.canonical.services.remote`."""

import io
import os
import tempfile
from unittest import TestCase

from ...domain import URI
from ..remote import RemoteSource


class TestCanResolve(TestCase):
    """Remote service can resolve HTTP URIs."""

    def setUp(self):
        """Given a remote service instance."""
        self.trusted_domain = 'arxiv.org'
        self.remote = RemoteSource(self.trusted_domain)

    def test_with_http_uri(self):
        """CAN resolve HTTP URIs in the trusted domain."""
        self.assertTrue(
            self.remote.can_resolve(URI('https://arxiv.org/stats/today'))
        )

    def test_with_http_uri_outside_trusted_domain(self):
        """Cannot resolve HTTP URIs outside of the trusted domain."""
        self.assertFalse(self.remote.can_resolve(URI('https://asdf.com')))

    def test_with_canonical_uri(self):
        """Cannot resolve canonical URIs."""
        self.assertFalse(self.remote.can_resolve(URI('arxiv:///foo/key')))

    def test_with_file_uri(self):
        """Cannot resolve file URIs."""
        self.assertFalse(self.remote.can_resolve(URI('/foo/bar/baz')))

# class TestLoadDeferred(TestCase):
#     """Filesystem service can load file URIs."""

#     def setUp(self):
#         """Given a file..."""
#         self.base_path = tempfile.mkdtemp()
#         self.filesystem = Filesystem(self.base_path)
#         _, self.file_path = tempfile.mkstemp(dir=self.base_path)
#         with open(self.file_path, 'wb') as f:
#             f.write(b'some content')

#     def test_load_deferred(self):
#         """Can load content from the file."""
#         resource = self.filesystem.load_deferred(URI(self.file_path))
#         self.assertEqual(resource.read(4), b'some')

#     def test_load_deferred_outside_base_path(self):
#         """Cannot load a file outside of the base path"""
#         _, other_path = tempfile.mkstemp()
#         with self.assertRaises(RuntimeError):
#             self.filesystem.load_deferred(URI(other_path))