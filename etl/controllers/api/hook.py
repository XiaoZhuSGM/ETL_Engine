from . import etl_api
from flask import current_app, request
from etl.service.login import LoginService
from .. import jsonify_with_error, APIError
service = LoginService()


@etl_api.before_request
def before_request():
    if current_app.debug is True:
        return
    token = request.headers.get("token")
    if not service.validate(token):
        return jsonify_with_error(APIError.UNAUTHORIZED)