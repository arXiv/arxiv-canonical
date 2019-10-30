"""Defines the HTTP routes and methods supported by the repository API."""

from flask import Blueprint, Response, request, send_file

from flask.json import jsonify

from .. import controllers


blueprint = Blueprint('api', __name__, url_prefix='')


@blueprint.route('/status', methods=['GET'])
def service_status() -> Response:
    """
    Service status endpoint.

    Returns ``200 OK`` if the service is up and ready to handle requests.
    """
    data, code, headers = controllers.service_status(request.params)
    response: Response = jsonify(data)
    response.status_code = code
    response.headers.extend(headers)  # type: ignore
    return response


@blueprint.route('/e-print/<arxiv:identifier>v<int:version>/events',
                 methods=['GET'])
def get_eprint_events(identifier: str, version: int) -> Response:
    """Get events for a specific version of an e-print."""
    data, code, headers = controllers.get_eprint_events(identifier, version)
    response: Response = jsonify(data)
    response.status_code = code
    response.headers.extend(headers)  # type: ignore
    return response


@blueprint.route('/e-print/<arxiv:identifier>v<int:version>/pdf',
                 methods=['GET'])
def get_eprint_pdf(identifier: str, version: int) -> Response:
    """Get PDF for a specific version of an e-print."""
    data, code, headers = controllers.get_eprint_pdf(identifier, version)
    pdf = data['metadata']
    response: Response = send_file(data['pointer'], as_attachment=True,
                                   attachment_filename=pdf.filename,
                                   last_modified=pdf.modified)
    response.status_code = code
    response.headers.extend(headers)  # type: ignore
    return response
