import io
from unittest import TestCase, mock

from ..readable import BytesIOProxy


class TestBytesIOProxy(TestCase):
    def setUp(self):
        """Create a new BytesIOProxy."""
        self.test_content = b'test content'
        self.mock_read = mock.MagicMock()
        self.mock_read.return_value = self.test_content
        self.readable = BytesIOProxy(self.mock_read)

    def test_read(self):
        """Read from a :class:`BytesIOProxy`."""
        self.assertEqual(self.mock_read.call_count, 0,
                         'Passed callable not yet used')
        self.assertEqual(self.readable.read(), self.test_content,
                         'Content is read from passed callable')
        self.assertEqual(self.mock_read.call_count, 1,
                         'Passed callable has been used')

    def test_read_again(self):
        """Read more than once from a :class:`BytesIOProxy`."""
        self.assertEqual(self.mock_read.call_count, 0,
                         'Passed callable not yet used')
        self.assertEqual(self.readable.read(), self.test_content,
                         'Content is read from passed callable')
        self.assertEqual(self.mock_read.call_count, 1,
                         'Passed callable has been used')
        self.readable.seek(0)
        self.assertEqual(self.readable.read(), self.test_content,
                         'The same content is read')
        self.assertEqual(self.mock_read.call_count, 1,
                         'Passed callable is not called a second time')

    def test_not_closed_before_loading_content(self):
        """BytesIOProxy is not closed prior to loading content."""
        self.assertFalse(self.readable.closed, 'Readable is not closed')

    def test_not_closed_after_loading_content(self):
        """BytesIOProxy is not closed after loading content."""
        self.assertEqual(self.readable.read(), self.test_content,
                         'Content is read from passed callable')
        self.assertFalse(self.readable.closed, 'Readable is not closed')

    def test_closed_after_explicit_close(self):
        """BytesIOProxy is closed after being explicitly closed."""
        self.assertFalse(self.readable.closed, 'Readable is not closed')
        self.readable.close()
        self.assertTrue(self.readable.closed, 'Readable is closed')

    def test_closed_after_read_and_explicit_close(self):
        """BytesIOProxy is closed after being explicitly closed."""
        self.assertEqual(self.readable.read(), self.test_content,
                         'Content is read from passed callable')
        self.assertFalse(self.readable.closed, 'Readable is not closed')
        self.readable.close()
        self.assertTrue(self.readable.closed, 'Readable is closed')

    def test_readable_before_loading_content(self):
        """BytesIOProxy is readable prior to loading content."""
        self.assertTrue(self.readable.readable(), 'Readable is readable')

    def test_readable_after_loading_content(self):
        """BytesIOProxy is readable after loading content."""
        self.assertEqual(self.readable.read(), self.test_content,
                         'Content is read from passed callable')
        self.assertTrue(self.readable.readable(), 'Readable is readable')

    def test_not_readable_after_explicit_close(self):
        """BytesIOProxy is not readable after being explicitly closed."""
        self.assertTrue(self.readable.readable(), 'Readable is readable')
        self.readable.close()
        with self.assertRaises(ValueError):
            self.readable.readable()

    def test_not_readable_after_read_and_explicit_close(self):
        """BytesIOProxy is not readable after being explicitly closed."""
        self.assertEqual(self.readable.read(), self.test_content,
                         'Content is read from passed callable')
        self.assertTrue(self.readable.readable(), 'Readable is readable')
        self.readable.close()
        with self.assertRaises(ValueError):
            self.readable.readable()