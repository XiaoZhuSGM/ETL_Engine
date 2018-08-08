from . import etl_admin_api
from flask import request, current_app
from etl.controllers import APIError, jsonify_with_error
from etl.service.login import LoginService
from etl.service.ext_table import ExtTableService
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


# 防止程序意外终端，导致抓表结构状态不对，在第一请求之前将所有running的状态改成fail
@etl_admin_api.before_app_first_request
def before_app_first_request():
    ext_table_service.set_all_fail()