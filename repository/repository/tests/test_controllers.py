"""Tests for :mod:`repository.controllers`."""

import io
from datetime import datetime
from http import HTTPStatus
from unittest import TestCase, mock

from pytz import UTC
from werkzeug.exceptions import NotFound

from arxiv.canonical import domain as D
from arxiv.canonical import NoSuchResource
from ..services.record import RepositoryService
from .. import controllers


class ControllerTestCase(TestCase):
    def setUp(self):
        self.mock_pdf = lambda *a, **k: io.BytesIO(b'foocontent')

        identifier = D.VersionedIdentifier('2901.00345v1')
        created = datetime(2029, 1, 29, 20, 4, 23, tzinfo=UTC)
        listing_id = D.ListingIdentifier.from_parts(created.date(), 'foo')

        self.render = D.CanonicalFile(
            filename='2901.00345v1.pdf',
            created=created,
            modified=created,
            size_bytes=404,
            content_type=D.ContentType.pdf,
            ref=D.URI('/fake/path.pdf')
        )

        self.version = D.Version(
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
            render=self.render
        )
        self.event = D.Event(
            identifier=identifier,
            event_date=created,
            event_type=D.EventType.NEW,
            categories=[D.Category('cs.DL')],
            version=self.version
        )
        self.version.events.append(self.event.summary)


class TestGetEPrintEvents(ControllerTestCase):
    """Tests for :func:`.controllers.get_eprint_events`."""

    @mock.patch(f'{controllers.__name__}.RepositoryService')
    def test_request_for_nonexistant_eprint(self, mock_RepositoryService):
        """A request is received for a nonexistant e-print."""
        mock_repo = mock.MagicMock()
        mock_repo.register.load_version.side_effect = NoSuchResource
        mock_RepositoryService.current_session.return_value = mock_repo
        with self.assertRaises(NotFound):
            controllers.get_eprint_events('1901.00123', 4)

    @mock.patch(f'{controllers.__name__}.RepositoryService')
    def test_request_for_existant_eprint(self, mock_RepositoryService):
        """A request is received for an existant e-print."""
        mock_repo = mock.MagicMock()
        mock_repo.register.load_version.return_value = self.version
        mock_RepositoryService.current_session.return_value = mock_repo

        data, code, headers = controllers.get_eprint_events('1901.00123', 4)

        self.assertIsInstance(data, list, 'Returns a list of objects')
        self.assertGreater(len(data), 0, 'Returns at least one object')
        for obj in data:
            self.assertIsInstance(obj, D.EventSummary,
                                  'List items are EventSummary')
        self.assertEqual(code, HTTPStatus.OK, 'Return status is 200 OK')


class TestGetEPrintPDF(ControllerTestCase):
    """Tests for :func:`.controllers.get_eprint_pdf`."""

    @mock.patch(f'{controllers.__name__}.RepositoryService')
    def test_request_for_nonexistant_eprint(self, mock_RepositoryService):
        """A request is received for a nonexistant e-print."""
        mock_repo = mock.MagicMock()
        mock_repo.register.load_render.side_effect = NoSuchResource
        mock_RepositoryService.current_session.return_value = mock_repo

        with self.assertRaises(NotFound):
            controllers.get_eprint_pdf('1901.00123', 4)

    @mock.patch(f'{controllers.__name__}.RepositoryService')
    def test_request_for_existant_eprint(self, mock_RepositoryService):
        """A request is received for an existant e-print."""
        mock_repo = mock.MagicMock()
        mock_repo.register.load_render.return_value \
            = self.render, io.BytesIO(b'foo')
        mock_RepositoryService.current_session.return_value = mock_repo

        data, code, headers = controllers.get_eprint_pdf('1901.00123', 4)
        self.assertIsInstance(data['metadata'], D.CanonicalFile,
                              'Returns a File')
        self.assertEqual(code, HTTPStatus.OK, 'Return status is 200 OK')
