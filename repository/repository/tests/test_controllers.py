"""Tests for :mod:`repository.controllers`."""

from unittest import TestCase, mock
from http import HTTPStatus

from werkzeug.exceptions import NotFound

from arxiv.canonical.domain import Event, CanonicalFile
from arxiv.canonical.services import store
from ..services.record import CanonicalStore
from .. import controllers


class TestGetEPrintEvents(TestCase):
    """Tests for :func:`.controllers.get_eprint_events`."""

    @mock.patch(f'{controllers.__name__}.CanonicalStore')
    def test_request_for_nonexistant_eprint(self, mock_CanonicalStore):
        """A request is received for a nonexistant e-print."""
        mock_CanonicalStore.current_session.return_value = mock.MagicMock(
            load_eprint=mock.MagicMock(side_effect=store.DoesNotExist)
        )
        with self.assertRaises(NotFound):
            controllers.get_eprint_events('1901.00123', 4)

    @mock.patch(f'{controllers.__name__}.CanonicalStore')
    def test_request_for_existant_eprint(self, mock_CanonicalStore):
        """A request is received for an existant e-print."""
        mock_CanonicalStore.current_session.return_value \
            = CanonicalStore.current_session()
        data, code, headers = controllers.get_eprint_events('1901.00123', 4)
        self.assertIsInstance(data, list, 'Returns a list of objects')
        self.assertGreater(len(data), 0, 'Returns at least one object')
        for obj in data:
            self.assertIsInstance(obj, Event, 'List items are Events')
        self.assertEqual(code, HTTPStatus.OK, 'Return status is 200 OK')


class TestGetEPrintPDF(TestCase):
    """Tests for :func:`.controllers.get_eprint_pdf`."""

    @mock.patch(f'{controllers.__name__}.CanonicalStore')
    def test_request_for_nonexistant_eprint(self, mock_CanonicalStore):
        """A request is received for a nonexistant e-print."""
        mock_CanonicalStore.current_session.return_value = mock.MagicMock(
            load_eprint=mock.MagicMock(side_effect=store.DoesNotExist)
        )
        with self.assertRaises(NotFound):
            controllers.get_eprint_pdf('1901.00123', 4)

    @mock.patch(f'{controllers.__name__}.CanonicalStore')
    def test_request_for_existant_eprint(self, mock_CanonicalStore):
        """A request is received for an existant e-print."""
        mock_CanonicalStore.current_session.return_value \
            = CanonicalStore.current_session()
        data, code, headers = controllers.get_eprint_pdf('1901.00123', 4)
        self.assertIsInstance(data, CanonicalFile, 'Returns a File')
        self.assertEqual(code, HTTPStatus.OK, 'Return status is 200 OK')
