"""Tests for :mod:`arxiv.canonical.services.remote`."""

import io
import os
import tempfile
from unittest import TestCase, mock

from ...domain import URI
from .. import remote


class TestCanResolve(TestCase):
    """Remote service can resolve HTTP URIs."""

    def setUp(self):
        """Given a remote service instance."""
        self.trusted_domain = 'arxiv.org'
        self.remote = remote.RemoteSource(self.trusted_domain, 'https')

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
        self.remote = remote.RemoteSource(self.trusted_domain, 'https')

    @mock.patch(f'{remote.__name__}.requests.Session')
    def test_load_deferred(self, mock_Session):
        """Can load content from the HTTP URI."""
        mock_session = mock.MagicMock()
        mock_session.get.return_value = mock.MagicMock(iter_content=iter(b'foocontent'))
        mock_Session.return_value = mock_session
        res = self.remote.load_deferred(URI('https://arxiv.org/stats/today'))
        self.assertEqual(res.read(4), b'<?xm')

    def test_load_deferred_outside_base_path(self):
        """Cannot load an HTTP URI outside trusted domain."""
        with self.assertRaises(RuntimeError):
            self.remote.load_deferred(URI('https://asdf.com'))