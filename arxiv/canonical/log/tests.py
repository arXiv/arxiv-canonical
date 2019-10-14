from datetime import datetime
import tempfile
from unittest import TestCase
from pytz import timezone
from . import log
from .log import D

ET = timezone('US/Eastern')


class TestLog(TestCase):
    def setUp(self):
        """Create a new log."""
        self.path = tempfile.mkdtemp()
        self.log = log.Log(self.path)

    def test_path(self):
        """Log paths based on the root path and current date."""
        self.assertEqual(
            self.log.current_log_path,
            f'{self.path}.{datetime.now(ET).date().isoformat()}.log'
        )

    def test_deref_success(self):
        """Log a successful dereference action."""
        vid = D.VersionedIdentifier('1902.00123v3')
        event_id = D.EventIdentifier.from_parts(vid, datetime.now(ET), 'foo')
        key = D.Key('file:///foo/baz/bat.tar.gz')

        entry = self.log.log_success(event_id, key, log.DEREFERENCE)

        self.assertEqual(self.log.read_last_entry().__dict__,
                         entry.__dict__,
                         "Logged success is the last entry")


