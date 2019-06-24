"""Defines the HTTP routes and methods supported by the repository API."""

from flask import Blueprint, Response, request
from flask.json import jsonify

from .. import controllers


blueprint = Blueprint('api', __name__, url_prefix='')


@blueprint.route('/status', methods=['GET'])
def service_status() -> Response:
    """
    Service status endpoint.

    Returns ``200 OK`` if the service is up and ready to handle requests.
    """
    response_data, status_code, headers = controllers.service_status(request.params)
    response: Response = jsonify(response_data)
    response.status_code = status_code
    response.headers.extend(headers)
    return response
