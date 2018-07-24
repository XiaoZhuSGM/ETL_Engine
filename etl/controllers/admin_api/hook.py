from . import etl_admin_api
from flask import request, current_app
from etl.controllers import APIError, jsonify_with_error
from etl.service.login import LoginService

service = LoginService()


@etl_admin_api.before_request
def before_request():
    if current_app.debug is True:
        return
    if request.endpoint in {"admin_api.login", "admin_api.ping"}:
        return
    token = request.headers.get("token")
    if not service.validate(token):
        return jsonify_with_error(APIError.UNAUTHORIZED)
