from unittest import TestCase

from ..content import ContentType, SourceFileType, SourceType


class TestSourceType(TestCase):
    """Tests for :class:`.SourceType`."""

    def test_available_formats(self):
        """Tests available formats based on source type."""
        self.assertListEqual(SourceType('I').available_formats, [])
        self.assertIn(ContentType.pdf, SourceType('IS').available_formats)
        self.assertIn(ContentType.ps, SourceType('IS').available_formats)

        self.assertIn(ContentType.pdf, SourceType('').available_formats)
        self.assertIn(ContentType.ps, SourceType('').available_formats)

        self.assertListEqual(SourceType('P').available_formats,
                             [ContentType.pdf, ContentType.ps])
        self.assertListEqual(SourceType('D').available_formats,
                             [ContentType.pdf])
        self.assertListEqual(SourceType('F').available_formats,
                             [ContentType.pdf])
        self.assertListEqual(SourceType('H').available_formats,
                             [ContentType.html])
        self.assertListEqual(SourceType('X').available_formats,
                             [ContentType.pdf])

        self.assertIn(ContentType.pdf, SourceType('').available_formats)
        self.assertIn(ContentType.ps, SourceType('').available_formats)
        self.assertIn(ContentType.dvi, SourceType('').available_formats)