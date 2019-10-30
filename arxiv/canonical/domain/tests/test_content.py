from unittest import TestCase

from ..content import ContentType, SourceFileType, SourceType, \
    available_formats_by_ext


class TestAvailableFormatsFromFilename(TestCase):

    def test_formats_from_source_file_name(self):
        """Test formats returned from file name."""
        self.assertListEqual(available_formats_by_ext('foo.pdf'),
                             [ContentType.pdf])
        self.assertListEqual(available_formats_by_ext('/bar.ps.gz'),
                             [ContentType.pdf, ContentType.ps])
        self.assertListEqual(available_formats_by_ext('abc.html.gz'),
                             [ContentType.html])

        # This differs from the implementation in arxiv-browse. It's not clear
        # why being gzipped or not should alter the way we handle an HTML
        # source file.
        self.assertListEqual(available_formats_by_ext('baz.html'),
                             [ContentType.html])

        self.assertIsNone(available_formats_by_ext(''))


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