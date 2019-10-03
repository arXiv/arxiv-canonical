"""Tests for :mod:`arxiv.canonical.services.remote`."""

import io
import os
import tempfile
from unittest import TestCase, mock

from ...domain import URI
from .. import repository


class TestCanResolve(TestCase):
    """Remote repository service can resolve arXiv canonical URIs."""

    def setUp(self):
        """Given a remote service instance."""
        self.trusted_domain = 'arxiv.org'
        self.remote = repository.RemoteRepository(self.trusted_domain, 'https')

    def test_with_http_uri(self):
        """Cannot resolve HTTP URIs."""
        self.assertFalse(
            self.remote.can_resolve(URI('https://arxiv.org/stats/today'))
        )

    def test_with_canonical_uri(self):
        """Can resolve canonical URIs."""
        self.assertTrue(self.remote.can_resolve(URI('arxiv:///foo/key')))

    def test_with_file_uri(self):
        """Cannot resolve file URIs."""
        self.assertFalse(self.remote.can_resolve(URI('/foo/bar/baz')))


class TestLoadDeferred(TestCase):
    """Remote service can load HTTP URIs."""

    @mock.patch(f'{remote.__name__}.requests.Session')
    def setUp(self, mock_Session):
        """Given a remote service instance."""
        self.mock_session = mock.MagicMock()
        mock_Session.return_value = self.mock_session

        self.trusted_domain = 'arxiv.org'
        self.remote = remote.RemoteSource(self.trusted_domain, 'https')

    def test_load_deferred(self):
        """Can load content from the HTTP URI."""
        mock_response = mock.MagicMock()
        mock_response.iter_content.return_value = \
            iter([b'foo', b'con' b'ten', b't'])
        self.mock_session.get.return_value = mock_response
        res = self.remote.load_deferred(URI('https://arxiv.org/stats/today'))
        self.assertEqual(self.mock_session.get.call_count, 0,
                         'No request is yet performed')
        self.assertEqual(res.read(4), b'fooc')
        self.assertEqual(self.mock_session.get.call_count, 1,
                         'Until an attempt to read() is made')

        mock_response.iter_content.return_value = \
            iter([b'foo', b'con' b'ten', b't'])
        res = self.remote.load_deferred(URI('https://arxiv.org/stats/today'))
        self.assertEqual(res.read(), b'foocontent')

    def test_load_deferred_outside_base_path(self):
        """Cannot load an HTTP URI outside trusted domain."""
        with self.assertRaises(RuntimeError):
            self.remote.load_deferred(URI('https://asdf.com'))