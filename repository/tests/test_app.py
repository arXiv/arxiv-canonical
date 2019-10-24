
import io
from datetime import datetime
from http import HTTPStatus
from unittest import TestCase, mock

from pytz import UTC

from arxiv.canonical import Primary
from arxiv.canonical import domain as D
from arxiv.canonical.services.store import InMemoryStorage

from repository.services import record
from repository.factory import create_api_app


class AppTestCase(TestCase):
    def setUp(self):
        self.mock_source = mock.MagicMock()
        self.mock_source.can_resolve.return_value = True

        self.mock_source.load_deferred = \
            lambda *a, **k: io.BytesIO(b'foocontent')

        self.storage = InMemoryStorage()
        self.primary = Primary(
            self.storage,
            [self.storage, self.mock_source],
            mock.MagicMock()
        )

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
                modified=created,
                size_bytes=4_304,
                content_type=D.ContentType.targz,
                ref=D.URI('/fake/path.tar.gz')
            ),
            render=D.CanonicalFile(
                filename='2901.00345v1.pdf',
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
        self.primary.register.add_events(self.event)

        self.app = create_api_app()
        record.RepositoryService.init_app(self.app)
        self.client = self.app.test_client()


class TestGetEPrintEvents(AppTestCase):
    """Requests for e-print events."""

    @mock.patch(f'{record.__name__}.CanonicalStore')
    def test_request_for_nonexistant_eprint(self, mock_CanonicalStore):
        """Get events for a nonexistant e-print."""
        mock_CanonicalStore.return_value = self.storage
        response = self.client.get('/e-print/1902.00123v4/events')
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    @mock.patch(f'{record.__name__}.CanonicalStore')
    def test_request_for_existant_eprint(self, mock_CanonicalStore):
        """A request is received for an existant e-print."""
        mock_CanonicalStore.return_value = self.storage
        response = self.client.get('/e-print/2901.00345v1/events')
        self.assertEqual(response.status_code, HTTPStatus.OK)


class TestGetEPrintPDF(AppTestCase):
    """Requests for e-print PDFs."""

    @mock.patch(f'{record.__name__}.CanonicalStore')
    def test_request_for_nonexistant_eprint(self, mock_CanonicalStore):
        """Get PDF for a nonexistant e-print."""
        mock_CanonicalStore.return_value = self.storage
        response = self.client.get('/e-print/1902.00123v4/pdf')
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    @mock.patch(f'{record.__name__}.CanonicalStore')
    def test_request_for_existant_eprint(self, mock_CanonicalStore):
        """A request is received for an existant e-print."""
        mock_CanonicalStore.return_value = self.storage
        response = self.client.get('/e-print/2901.00345v1/pdf')
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.headers['Content-Disposition'],
                         'attachment; filename=2901.00345v1.pdf')
        self.assertEqual(response.data, b'foocontent')
