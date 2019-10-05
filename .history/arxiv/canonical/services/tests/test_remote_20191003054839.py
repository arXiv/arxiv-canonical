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
        self.remote = RemoteSource(self.trusted_domain, 'https')

    def test_with_http_uri(self):
        """CAN resolve HTTP URIs in the trusted domain."""
        self.assertTrue(
            self.remote.can_resolve(URI('https://arxiv.org/stats/today'))
        )

    def test_with_http_uri_outside_trusted_domain(self):
        """Cannot resolve HTTP URIs outside of the trusted domain."""
        self.assertFalse(self.remote.can_resolve(URI('https://asdf.com')))

    def test_with_http_uri_with_nontrusted_scheme(self):
        """Cannot resolve HTTP URIs with a non-trusted scheme."""
        self.assertFalse(
            self.remote.can_resolve(URI('http://arxiv.org/stats/today'))
        )

    def test_with_canonical_uri(self):
        """Cannot resolve canonical URIs."""
        self.assertFalse(self.remote.can_resolve(URI('arxiv:///foo/key')))

    def test_with_file_uri(self):
        """Cannot resolve file URIs."""
        self.assertFalse(self.remote.can_resolve(URI('/foo/bar/baz')))


class TestLoadDeferred(TestCase):
    """Remote service can load HTTP URIs."""

    def setUp(self):
        """Given a remote service instance."""
        self.trusted_domain = 'arxiv.org'
        self.remote = RemoteSource(self.trusted_domain, 'https')

    def test_load_deferred(self):
        """Can load content from the HTTP URI."""
        res = self.remote.load_deferred(URI('http://arxiv.org/stats/today'))
        self.assertEqual(res.read(4), b'<htm')

    def test_load_deferred_outside_base_path(self):
        """Cannot load an HTTP URI outside trusted domain."""
        with self.assertRaises(RuntimeError):
            self.remote.load_deferred(URI('https://asdf.com'))