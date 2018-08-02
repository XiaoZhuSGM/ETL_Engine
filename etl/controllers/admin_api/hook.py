from . import etl_admin_api
from flask import request, current_app
from etl.controllers import APIError, jsonify_with_error
from etl.service.login import LoginService
from etl.service.ext_table import ExtTableService
import fileinput
import os
service = LoginService()
ext_table_service = ExtTableService()


@etl_admin_api.before_request
def before_request():
    if current_app.debug is True:
        return
    if request.endpoint in {"admin_api.login", "admin_api.ping"}:
        return
    token = request.headers.get("token")
    if not service.validate(token):
        return jsonify_with_error(APIError.UNAUTHORIZED)


@etl_admin_api.before_app_first_request
def before_app_first_request():
    if os.path.exists(ext_table_service.status_file_path):
        with fileinput.input(files=ext_table_service.status_file_path, inplace=True) as file:
            for line in file:
                print(line.strip().replace('running', 'fail'))
