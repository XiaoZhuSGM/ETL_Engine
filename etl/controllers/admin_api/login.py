from flask import request

from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.service.login import LoginService, LoginFailed
from etl.validators import validate_arg
from etl.validators.login import LoginInput
from . import etl_admin_api

service = LoginService()


@etl_admin_api.route("/login", methods=["POST"])
@validate_arg(LoginInput)
def login():
    data = request.json
    try:
        token = service.login(data["username"], data["password"])
        return jsonify_with_data(APIError.OK, data={"token": token})
    except LoginFailed as e:
        return jsonify_with_error(APIError.UNAUTHORIZED)
