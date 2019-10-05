import io
from datetime import datetime
from pytz import UTC
from typing import Callable, Tuple, Dict, List, IO
from unittest import TestCase, mock

from ...register.api import (RegisterAPI, ICanonicalSource, ICanonicalStorage,
                             IStorableEntry, Manifest, NoSuchResource, D, R)
from ...services.store import InMemoryStorage
from ..register import Reader, Writer
from ..role import Primary, Repository


class TestPrimary(TestCase):
    """Primary has access to the read and write API for the register."""

    def setUp(self):
        self.mock_source = mock.MagicMock(spec=ICanonicalSource)
        self.mock_source.can_resolve.return_value = True

        self.mock_source.load_deferred = \
            lambda *a, **k: io.BytesIO(b'foocontent')
        self.storage = InMemoryStorage()
        self.primary = Primary(self.storage, [self.storage, self.mock_source], mock.MagicMock())
        self.repository = Repository(self.storage, [self.storage, self.mock_source], mock.MagicMock())

        identifier = D.VersionedIdentifier('2901.00345v1')
        created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
        listing_id = D.ListingIdentifier.from_parts(created.date(), 'foo')

        version = D.Version(
            identifier=identifier,
            announced_date=created.date(),
            announced_date_first=created.date(),
            submitted_date=created,
            updated_date=created,
            is_announced=True,
            events=[],
            previous_versions=[],
            metadata=D.Metadata(
                primary_classification=D.Category('cs.DL'),
                secondary_classification=[D.Category('cs.IR')],
                title='Foo title',
                abstract='It is abstract',
                authors='Ima N. Author (FSU)',
                license=D.License(href="http://some.license")
            ),
            source=D.CanonicalFile(
                filename='2901.00345v1.tar.gz',
                created=created,
                modified=created,
                size_bytes=4_304,
                content_type=D.ContentType.targz,
                ref=D.URI('/fake/path.tar.gz')
            ),
            render=D.CanonicalFile(
                filename='2901.00345v1.pdf',
                created=created,
                modified=created,
                size_bytes=404,
                content_type=D.ContentType.pdf,
                ref=D.URI('/fake/path.pdf')
            )
        )
        self.event = D.Event(
            identifier=identifier,
            event_date=created,
            event_type=D.EventType.NEW,
            categories=[D.Category('cs.DL')],
            version=version
        )
        self.timestamp = created
        self.event_date = self.timestamp.date()

    def test_add_load_event(self):
        """Primary can add + load an event; repository can load but not add."""
        self.primary.register.add_events(self.event)

        with self.assertRaises(AttributeError):
            self.repository.register.add_events(self.event)
        event_id = self.event.event_id
        self.assertEqual(self.primary.register.load_event(event_id),
                         self.event,
                        'Added event can be loaded again')
        self.assertEqual(self.repository.register.load_event(event_id),
                         self.event,
                        'Added event can be loaded again')

    def test_add_load_events_by_date(self):
        """Can add events and load them using date selector."""
        events, N = self.primary.register.load_events(self.event_date)
        self.assertEqual(N, 0, 'There are no events')
        self.assertEqual(len(list(events)), N, 'There are truly no events')

        events, N = self.repository.register.load_events(self.event_date)
        self.assertEqual(N, 0, 'There are no events')
        self.assertEqual(len(list(events)), N, 'There are truly no events')

        self.primary.register.add_events(self.event)

        events, N = self.primary.register.load_events(self.event_date)
        self.assertEqual(N, 1, 'There is now one event')
        event_list = list(events)
        self.assertEqual(len(event_list), N, 'There is truly 1 event')
        self.assertEqual(event_list[0], self.event,
                         'And that event is the one that we just added')

        events, N = self.repository.register.load_events(self.event_date)
        self.assertEqual(N, 1, 'There is now one event')
        event_list = list(events)
        self.assertEqual(len(event_list), N, 'There is truly 1 event')
        self.assertEqual(event_list[0], self.event,
                         'And that event is the one that we just added')

        events, N = self.primary.register.load_events(datetime.now().date())
        self.assertEqual(N, 0, 'But there are no events from today')
        self.assertEqual(len(list(events)), N, 'Indeed, no events')

        events, N = self.repository.register.load_events(datetime.now().date())
        self.assertEqual(N, 0, 'But there are no events from today')
        self.assertEqual(len(list(events)), N, 'Indeed, no events')

    def test_add_load_events_by_month(self):
        """Can add events and load them using month selector."""
        events, N = self.primary.register.load_events((self.event_date.year,
                                          self.event_date.month))
        self.assertEqual(N, 0, 'There are no events')
        self.assertEqual(len(list(events)), N, 'There are truly no events')

        self.primary.register.add_events(self.event)

        events, N = self.primary.register.load_events((self.event_date.year,
                                          self.event_date.month))
        self.assertEqual(N, 1, 'There is now one event')
        event_list = list(events)
        self.assertEqual(len(event_list), N, 'There is truly 1 event')
        self.assertEqual(event_list[0], self.event,
                         'And that event is the one that we just added')

        events, N = self.primary.register.load_events((datetime.now().year,
                                                       datetime.now().month))
        self.assertEqual(N, 0, 'But there are no events from this month')
        self.assertEqual(len(list(events)), N, 'Indeed, no events')

    def test_add_load_events_by_year(self):
        """Can add events and load them using year selector."""
        events, N = self.primary.register.load_events(self.event_date.year)
        self.assertEqual(N, 0, 'There are no events')
        self.assertEqual(len(list(events)), N, 'There are truly no events')

        self.primary.register.add_events(self.event)

        events, N = self.primary.register.load_events(self.event_date.year)
        self.assertEqual(N, 1, 'There is now one event')
        event_list = list(events)
        self.assertEqual(len(event_list), N, 'There is truly 1 event')
        self.assertEqual(event_list[0], self.event,
                         'And that event is the one that we just added')

        events, N = self.primary.register.load_events(datetime.now().year)
        self.assertEqual(N, 0, 'But there are no events from this year')
        self.assertEqual(len(list(events)), N, 'Indeed, no events')

    def test_can_load_listing(self):
        """Can load listings."""
        listing = self.primary.register.load_listing(self.event_date)
        self.assertEqual(len(listing.events), 0, 'Listing has no events')

        self.primary.register.add_events(self.event)

        listing = self.primary.register.load_listing(self.event_date)
        self.assertEqual(len(listing.events), 1,
                         'But it has one after we add an event')
        self.assertEqual(listing.events[0], self.event,
                         'And it is the event that we added')

    def test_can_load_version(self):
        """Can load a Version that was created via an event."""
        with self.assertRaises(Exception):
            self.primary.register.load_version(self.event.identifier)

        self.primary.register.add_events(self.event)

        version = self.primary.register.load_version(self.event.identifier)
        self.assertEqual(version, self.event.version,
                         'Can load the Version that we just added')

    def test_can_load_eprint(self):
        """Can load an EPrint that was created via an event."""
        with self.assertRaises(Exception):
            self.primary.register.load_eprint(self.event.identifier.arxiv_id)

        self.primary.register.add_events(self.event)

        eprint = self.primary.register.load_eprint(self.event.identifier.arxiv_id)
        self.assertEqual(eprint.versions[self.event.identifier],
                         self.event.version,
                         'Can load the Version that we just added')

    def test_can_load_history(self):
        """Can load the event history of a Version or EPrint."""
        with self.assertRaises(NoSuchResource):
            self.primary.register.load_history(self.event.identifier.arxiv_id)
        with self.assertRaises(NoSuchResource):
            self.primary.register.load_history(self.event.identifier)

        self.primary.register.add_events(self.event)

        summary = next(
            self.primary.register.load_history(self.event.identifier.arxiv_id)
        )
        self.assertEqual(summary, self.event.summary,
                         'History includes a summary of our event')

        summary = next(
            self.primary.register.load_history(self.event.identifier)
        )
        self.assertEqual(summary, self.event.summary,
                         'History includes a summary of our event')

    def test_can_load_render(self):
        """Can load an EPrint that was created via an event."""
        with self.assertRaises(NoSuchResource):
            self.primary.register.load_render(self.event.identifier)

        self.primary.register.add_events(self.event)

        cf, content = self.primary.register.load_render(self.event.identifier)
        self.assertEqual(cf, self.event.version.render,
                         'Loads the render file')
        self.assertEqual(content.read(), b'foocontent', 'Loads render content')

        cf, content = self.primary.register.load_source(self.event.identifier)
        self.assertEqual(cf, self.event.version.source,
                         'Loads the source file')
        self.assertEqual(content.read(), b'foocontent', 'Loads source content')



