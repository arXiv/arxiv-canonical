"""Tests for :mod:`arxiv.canonical.domain`."""

from datetime import datetime
from unittest import TestCase

from ..file import URI, Key, CanonicalFile, ContentType


class TestURIForFile(TestCase):
    """URI can refer to a local file."""

    def test_file_uri(self):
        """URI is initialized with an absolute path."""
        path = '/path/to/some/data'
        uri = URI(path)
        self.assertTrue(uri.is_file, 'Recognized as a file reference')
        self.assertFalse(uri.is_http_url, 'Not an HTTP URI')
        self.assertFalse(uri.is_canonical, 'Not a canonical URI')
        self.assertEqual(uri.scheme, 'file')
        self.assertEqual(uri.path, path, 'Original path is preserved')

    def test_file_uri_with_relative_path(self):
        """URI is initialized with a relative path."""
        path = 'path/to/some/data'
        with self.assertRaises(ValueError):
            URI(path)


class TestCanonicalURI(TestCase):
    """URI can refer to a canonical resource."""

    def test_canonical_uri(self):
        """URI is initialized with an arXiv canonical URI."""
        raw = 'arxiv:///path/to/a/resource'
        uri = URI(raw)
        self.assertFalse(uri.is_file, 'Not a local file reference')
        self.assertFalse(uri.is_http_url, 'Not an HTTP URI')
        self.assertTrue(uri.is_canonical, 'Recognized as a canonical URI')
        self.assertEqual(uri.scheme, 'arxiv')
        self.assertEqual(uri.path, '/path/to/a/resource')


class TestHTTPURI(TestCase):
    """URI can refer to an HTTP URI."""

    def test_valid_http_uri(self):
        """URI is initialized with a valid HTTP URI."""
        raw = 'http://asdf.com'
        uri = URI(raw)
        self.assertFalse(uri.is_file, 'Not a local file reference')
        self.assertTrue(uri.is_http_url, 'Recognized as an HTTP URI')
        self.assertFalse(uri.is_canonical, 'Not a canonical URI')
        self.assertEqual(uri.scheme, 'http')

    def test_valid_https_uri(self):
        """URI is initialized with a valid HTTPS URI."""
        raw = 'https://asdf.com'
        uri = URI(raw)
        self.assertFalse(uri.is_file, 'Not a local file reference')
        self.assertTrue(uri.is_http_url, 'Recognized as an HTTP URI')
        self.assertFalse(uri.is_canonical, 'Not a canonical URI')
        self.assertEqual(uri.scheme, 'https')

    def test_valid_ftp_uri(self):
        """URI is initialized with a valid FTP URI."""
        raw = 'ftp://asdf.com/foo'
        uri = URI(raw)
        self.assertFalse(uri.is_file, 'Not a local file reference')
        self.assertFalse(uri.is_http_url, 'Not an HTTP URI')
        self.assertFalse(uri.is_canonical, 'Not a canonical URI')
        self.assertEqual(uri.scheme, 'ftp')


class TestKey(TestCase):
    """Key is a canonical URI."""

    def test_with_absolute_path(self):
        """Key is initialized with an absolute path."""
        raw = '/path/to/a/resource'
        key = Key(raw)
        self.assertTrue(key.is_canonical, 'Key is a canonical URI')
        self.assertIsInstance(key, URI, 'Indeed, it is an URI')
        self.assertEqual(key.scheme, 'arxiv')
        self.assertEqual(str(key), f'arxiv://{raw}')


class TestCanonicalFile(TestCase):
    def test_dict_transformation(self):
        """Transformation to/from dict preserves state."""
        f = CanonicalFile(
            created=datetime.now(),
            modified=datetime.now(),
            size_bytes=5_324,
            content_type=ContentType.json,
            filename='foo.json',
            ref=URI('arxiv:///key/for/foo.json')
        )
        self.assertEqual(f, CanonicalFile.from_dict(f.to_dict()))

