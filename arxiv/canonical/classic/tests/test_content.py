"""Tests for :mod:`arxiv.canonical.classic.content`."""

from unittest import TestCase

from .. import content
from ... import domain as D


class TestGetRemoteContent(TestCase):
    """Test getting content from arxiv.org."""

    def test_get_via_http(self):
        """Get metadata about a PDF via HTTP."""
        cf = content._get_via_http(D.VersionedIdentifier('0801.1021v2'),
                                   D.ContentType.pdf)
        self.assertEqual(cf.size_bytes, 237187)
        self.assertEqual(cf.content_type, D.ContentType.pdf)
        self.assertTrue(cf.filename.endswith(D.ContentType.pdf.ext))
        self.assertEqual(cf.ref,
                         D.URI('https://arxiv.org/pdf/0801.1021v2.pdf'))
