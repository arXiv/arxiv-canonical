"""
Request controllers for the repository service.

These are used to handle requests originating from :mod:`.routes.api`.
"""

from typing import Tuple, Any, Dict, Union
from http import HTTPStatus

from werkzeug.datastructures import MultiDict
from werkzeug.exceptions import NotFound

from arxiv.canonical.domain import CanonicalFile, VersionedIdentifier

from .services.record import RepositoryService, NoSuchResource


Response = Tuple[Dict[str, Any], HTTPStatus, Dict[str, str]]


def service_status(_: MultiDict) -> Response:
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
    repo = RepositoryService.current_session()
    try:
        v_identifier = VersionedIdentifier.from_parts(identifier, version)
    except (TypeError, ValueError) as e:
        raise NotFound(f'No such e-print: {identifier}v{version}') from e
    try:
        eprint = repo.register.load_version(v_identifier)
    except NoSuchResource as e:
        raise NotFound(f'No such e-print: {identifier}v{version}') from e
    return eprint.events, HTTPStatus.OK, {}


def get_eprint_pdf(identifier: str, version: int) -> Response:
    """
    Retrieve pdf for a specific e-print version.

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
    repo = RepositoryService.current_session()
    try:
        v_identifier = VersionedIdentifier.from_parts(identifier, version)
    except (ValueError, TypeError) as e:
        raise NotFound(f'No such e-print: {identifier}v{version}') from e
    try:
        cf, f = repo.register.load_render(v_identifier)
    except NoSuchResource as e:
        raise NotFound(f'No such e-print: {identifier}v{version}') from e
    return {'metadata': cf, 'pointer': f}, HTTPStatus.OK, {}
