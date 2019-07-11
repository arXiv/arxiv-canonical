
from unittest import TestCase, mock
from http import HTTPStatus

from repository.services.record import CanonicalStore, DoesNotExist
from repository.factory import create_api_app


class TestGetEPrintEvents(TestCase):
    """Requests for e-print events."""

    def setUp(self):
        """Spin up an app."""
        self.app = create_api_app()
        self.client = self.app.test_client()

    @mock.patch(f'repository.controllers.CanonicalStore')
    def test_request_for_nonexistant_eprint(self, mock_CanonicalStore):
        """Get events for a nonexistant e-print."""
        mock_CanonicalStore.current_session.return_value = mock.MagicMock(
            load_eprint=mock.MagicMock(side_effect=DoesNotExist)
        )
        response = self.client.get('/e-print/1902.00123v4/events')
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
    
    @mock.patch(f'repository.controllers.CanonicalStore')
    def test_request_for_existant_eprint(self, mock_CanonicalStore):
        """A request is received for an existant e-print."""
        mock_CanonicalStore.current_session.return_value \
            = CanonicalStore.current_session()
        
        response = self.client.get('/e-print/1902.00123v4/events')
        self.assertEqual(response.status_code, HTTPStatus.OK)


class TestGetEPrintPDF(TestCase):
    """Requests for e-print PDFs."""

    def setUp(self):
        """Spin up an app."""
        self.app = create_api_app()
        self.client = self.app.test_client()

    @mock.patch(f'repository.controllers.CanonicalStore')
    def test_request_for_nonexistant_eprint(self, mock_CanonicalStore):
        """Get PDF for a nonexistant e-print."""
        mock_CanonicalStore.current_session.return_value = mock.MagicMock(
            load_eprint=mock.MagicMock(side_effect=DoesNotExist)
        )
        response = self.client.get('/e-print/1902.00123v4/pdf')
        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    @mock.patch(f'repository.controllers.CanonicalStore')
    def test_request_for_existant_eprint(self, mock_CanonicalStore):
        """A request is received for an existant e-print."""
        mock_CanonicalStore.current_session.return_value \
            = CanonicalStore.current_session()
        response = self.client.get('/e-print/1902.00123v4/pdf')
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.headers['Content-Disposition'],
                         'attachment; filename=1901.00123.pdf')
        self.assertEqual(response.data, b'foopdf')
