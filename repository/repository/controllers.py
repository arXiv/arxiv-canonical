"""
Request controllers for the repository service.

These are used to handle requests originating from :mod:`.routes.api`.
"""

from typing import Tuple, Any, Dict
from http import HTTPStatus

from werkzeug.datastructures import MultiDict

from arxiv.canonical.services import MockCanonicalStore


Response = Tuple[Dict[str, Any], HTTPStatus, Dict[str, str]]


def service_status(params: MultiDict) -> Response:
    """
    Handle requests for the service status endpoint.

    Returns ``200 OK`` if the service is up and ready to handle requests.
    """
    return {'iam': 'ok'}, HTTPStatus.OK, {}