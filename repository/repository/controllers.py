"""
Request controllers for the repository service.

These are used to handle requests originating from :mod:`.routes.api`.
"""

from typing import Tuple, Any, Dict
from http import HTTPStatus

from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import NotFound

from arxiv.canonical.services.store import FakeCanonicalStore as CanonicalStore
from arxiv.canonical.services.store import DoesNotExist



Response = Tuple[Dict[str, Any], HTTPStatus, Dict[str, str]]


def service_status(params: MultiDict) -> Response:
    """
    Handle requests for the service status endpoint.

    Returns ``200 OK`` if the service is up and ready to handle requests.
    """
    return {'iam': 'ok'}, HTTPStatus.OK, {}


def get_eprint_events(identifier: str, version: int) -> Response:
    """
    Retrieve events for a specific e-print version.

    Parameters
    ----------
    identifier : str
        A valid arXiv identifier.
    version : int
        Numeric version of the e-print.

    Raises
    ------
    :class:`.NotFound`
        Raised when the requested identifier + version does not exist.

    """
    estore = CanonicalStore.current_session()
    try:
        eprint = estore.load_eprint(identifier, version)
    except DoesNotExist as e:
        raise NotFound(f'No such e-print: {identifier}v{version}') from e
    return eprint.history, HTTPStatus.OK, {}
    