"""Tests for :mod:`arxiv.canonical.classic.content`."""

import os
from datetime import datetime
from os.path import join
import shutil
import tempfile
from unittest import TestCase, mock

from pytz import UTC

from .. import content
from ... import domain as D


def touch(path):
    parent, _ = os.path.split(path)
    if not os.path.exists(parent):
        os.makedirs(parent)
    with open(path, 'wb') as f:
        f.write(b'')


class TestGetFormats(TestCase):
    """Get the dissemination formats for a version."""

    def setUp(self):
        """Make the classic file tree."""
        self.data_path = tempfile.mkdtemp()
        self.ori = join(self.data_path, 'orig')
        self.ftp = join(self.data_path, 'ftp')
        os.makedirs(self.ori)
        os.makedirs(self.ftp)
        self.cache_path = tempfile.mkdtemp()
        self.psc = join(self.cache_path, 'ps_cache')
        os.makedirs(self.psc)

    @mock.patch(f'{content.__name__}.RemoteSourceWithHead')
    def test_get_v1_single_format(self, mock_RemoteSourceWithHead):
        """Get the first version of a multi-version e-print with one format."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        touch(pdf_path)

        # HTTP fallback does not yield any additional formats.
        mock_remote_source = mock.MagicMock()
        mock_remote_source.head.return_value = None
        mock_RemoteSourceWithHead.return_value = mock_remote_source

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        self.assertEqual(len(cfs), 1)
        self.assertEqual(cfs[0].content_type, D.ContentType.pdf)
        self.assertFalse(cfs[0].is_gzipped)
        self.assertEqual(cfs[0].size_bytes, 0)
        self.assertEqual(cfs[0].ref.path, pdf_path)

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_two_formats(self, mock_remote_source):
        """Get the first version of an e-print with two formats."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback does not yield any additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        self.assertEqual(len(cfs), 2)
        self.assertIn(D.ContentType.pdf, [cf.content_type for cf in cfs])
        self.assertIn(D.ContentType.ps, [cf.content_type for cf in cfs])
        for cf in cfs:
            self.assertEqual(cf.size_bytes, 0)
            if cf.content_type == D.ContentType.pdf:
                self.assertFalse(cf.is_gzipped)
                self.assertEqual(cf.ref.path, pdf_path)
            elif cf.content_type == D.ContentType.ps:
                self.assertTrue(cf.is_gzipped)
                self.assertEqual(cf.ref.path, ps_path)

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_with_one_remote(self, mock_remote_source):
        """Get formats for the first version with one local format missing."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback yields one additional format.
        mock_remote_source.head.return_value = D.CanonicalFile(
            modified=datetime.now(UTC),
            size_bytes=42,
            content_type=D.ContentType.dvi,
            ref=D.URI('https://arxiv.org/dvi/1901.00123v1'),
            filename='1901.00123v1.dvi',
            is_gzipped=True
        )

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        self.assertEqual(len(cfs), 3)
        self.assertIn(D.ContentType.pdf, [cf.content_type for cf in cfs])
        self.assertIn(D.ContentType.ps, [cf.content_type for cf in cfs])
        self.assertIn(D.ContentType.dvi, [cf.content_type for cf in cfs])
        for cf in cfs:
            if cf.content_type == D.ContentType.dvi:
                self.assertTrue(cf.is_gzipped)
                self.assertEqual(cf.ref.path, '/dvi/1901.00123v1')
                self.assertEqual(cf.size_bytes, 42)

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_source_encrypted(self, mock_remote_source):
        """Get formats for a source-encrypted version."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback yields no additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('IS')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        # Finds postscript and pdf formats.
        self.assertEqual(len(cfs), 2)
        self.assertIn(D.ContentType.pdf, [cf.content_type for cf in cfs])
        self.assertIn(D.ContentType.ps, [cf.content_type for cf in cfs])

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_ignore(self, mock_remote_source):
        """Get formats for an ignore-type version."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback yields no additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('I')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        # Finds no formats.
        self.assertEqual(len([o for o in formats]), 0)

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_ps_only(self, mock_remote_source):
        """Get formats for a ps-only version."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback yields no additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('P')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        # Finds postscript and pdf formats.
        self.assertEqual(len(cfs), 2)
        self.assertIn(D.ContentType.pdf, [cf.content_type for cf in cfs])
        self.assertIn(D.ContentType.ps, [cf.content_type for cf in cfs])

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_pdflatex(self, mock_remote_source):
        """Get formats for a pdflatex version."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback yields no additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('D')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        # Finds  pdf format.
        self.assertEqual(len(cfs), 1)
        self.assertIn(D.ContentType.pdf, [cf.content_type for cf in cfs])

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_pdf_only(self, mock_remote_source):
        """Get formats for a pdfladtex version."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        pdf_path = join(self.psc, 'arxiv', 'pdf', '1901', '1901.00123v1.pdf')
        ps_path = join(self.psc, 'arxiv', 'ps', '1901', '1901.00123v1.ps.gz')
        touch(pdf_path)
        touch(ps_path)

        # HTTP fallback yields no additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('F')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        # Finds pdf format.
        self.assertEqual(len(cfs), 1)
        self.assertIn(D.ContentType.pdf, [cf.content_type for cf in cfs])

    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_html(self, mock_remote_source):
        """Get formats for a multi-file HTML version."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        html_path = join(self.psc, 'arxiv', 'html', '1901', '1901.00123v1',
                         'fooPaper.html')

        touch(html_path)

        # HTTP fallback yields no additional formats.
        mock_remote_source.head.return_value = None

        identifier = D.VersionedIdentifier('1901.00123v1')

        source_type = D.SourceType('H')
        source_file = content.get_source(self.data_path, identifier)
        formats = content.get_formats(self.data_path, self.cache_path,
                                      identifier, source_type, source_file)

        cfs = [o for o in formats]
        # Finds html format.
        self.assertEqual(len(cfs), 1)
        self.assertIn(D.ContentType.html, [cf.content_type for cf in cfs])

    # TODO: implement this test!
    @mock.patch(f'{content.__name__}.REMOTE')
    def test_get_v1_docx(self, mock_remote_source):
        """Get formats for a DOCX version."""
        source_type = D.SourceType('H')


class TestGetSource(TestCase):
    """Get the source for a version."""

    def setUp(self):
        """Make the classic file tree."""
        self.data_path = tempfile.mkdtemp()
        self.ori = join(self.data_path, 'orig')
        self.ftp = join(self.data_path, 'ftp')
        os.makedirs(self.ori)
        os.makedirs(self.ftp)
        self.cache_path = tempfile.mkdtemp()
        self.psc = join(self.cache_path, 'ps_cache')
        os.makedirs(self.psc)

    def test_get_v1_of_multiple(self):
        """Get the first labeled version of a multi-version e-print."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        path = join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz')
        touch(path)
        identifier = D.VersionedIdentifier('1901.00123v1')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tar)
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v1_of_multiple_without_extension(self):
        """Get the first version, lacking an extension."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        path = join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.gz')
        touch(path)
        identifier = D.VersionedIdentifier('1901.00123v1')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tex,
                         'We assume that it is a TeX source.')
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v1_of_multiple_old_style(self):
        """Get the first labeled version of a multi-version e-print."""
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v1.abs'))
        path = join(self.ori, 'math', 'papers', '9501', '95010123v1.tar.gz')
        touch(path)
        identifier = D.VersionedIdentifier('math/95010123v1')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tar)
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v3_the_latest(self):
        """Get the third version, the most recent."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.tar.gz'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v2.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v2.ps.gz'))

        touch(join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.abs'))
        path = join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.tar.gz')
        touch(path)

        identifier = D.VersionedIdentifier('1901.00123v3')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tar)
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v3_the_latest_old_style(self):
        """Get the third version, the most recent."""
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v1.abs'))
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v1.tar.gz'))
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v2.abs'))
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v2.tar.gz'))

        touch(join(self.ftp, 'math', 'papers', '9501', '95010123.abs'))
        path = join(self.ftp, 'math', 'papers', '9501', '95010123.tar.gz')
        touch(path)

        identifier = D.VersionedIdentifier('math/95010123v3')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tar)
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v1_the_only(self):
        """Get the first and only version."""
        touch(join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.abs'))
        path = join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.tar.gz')
        touch(path)

        identifier = D.VersionedIdentifier('1901.00123v1')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tar)
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v1_the_only_old_style(self):
        """Get the first and only version."""
        touch(join(self.ftp, 'math', 'papers', '9501', '95010123.abs'))
        path = join(self.ftp, 'math', 'papers', '9501', '95010123.tar.gz')
        touch(path)

        identifier = D.VersionedIdentifier('math/95010123v1')

        source = content.get_source(self.data_path, identifier)

        self.assertEqual(source.content_type, D.ContentType.tar)
        self.assertTrue(source.is_gzipped)
        self.assertEqual(source.ref.path, path)

    def test_get_v2_nonexistant(self):
        """Get a version that does not exist."""
        touch(join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.abs'))
        touch(join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.tar.gz'))

        with self.assertRaises(IOError):
            content.get_source(self.data_path,
                               D.VersionedIdentifier('1901.00123v2'))

    def test_get_v2_nonexistant_old_style(self):
        """Get a version that does not exist."""
        touch(join(self.ftp, 'math', 'papers', '9501', '95010123.abs'))
        touch(join(self.ftp, 'math', 'papers', '9501', '95010123.tar.gz'))

        with self.assertRaises(IOError):
            content.get_source(self.data_path,
                               D.VersionedIdentifier('math/95010123v2'))

    def test_get_v3_nonexistant(self):
        """Get a version that does not exist."""
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.abs'))
        touch(join(self.ori, 'arxiv', 'papers', '1901', '1901.00123v1.ps.gz'))
        touch(join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.abs'))
        touch(join(self.ftp, 'arxiv', 'papers', '1901', '1901.00123.tar.gz'))

        with self.assertRaises(IOError):
            content.get_source(self.data_path,
                               D.VersionedIdentifier('1901.00123v3'))

    def test_get_v3_nonexistant_old_style(self):
        """Get a version that does not exist."""
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v1.abs'))
        touch(join(self.ori, 'math', 'papers', '9501', '95010123v1.ps.gz'))
        touch(join(self.ftp, 'math', 'papers', '9501', '95010123.abs'))
        touch(join(self.ftp, 'math', 'papers', '9501', '95010123.tar.gz'))

        with self.assertRaises(IOError):
            content.get_source(self.data_path,
                               D.VersionedIdentifier('math/95010123v3'))

    def tearDown(self):
        shutil.rmtree(self.data_path)
        shutil.rmtree(self.cache_path)



# class TestGetRemoteContent(TestCase):
#     """Test getting content from arxiv.org."""

#     def test_get_via_http(self):
#         """Get metadata about a PDF via HTTP."""
#         cf = content._get_via_http(D.VersionedIdentifier('0801.1021v2'),
#                                    D.ContentType.pdf)
#         self.assertEqual(cf.size_bytes, 237187)
#         self.assertEqual(cf.content_type, D.ContentType.pdf)
#         self.assertTrue(cf.filename.endswith(D.ContentType.pdf.ext))
#         self.assertEqual(cf.ref,
#                          D.URI('https://arxiv.org/pdf/0801.1021v2.pdf'))
