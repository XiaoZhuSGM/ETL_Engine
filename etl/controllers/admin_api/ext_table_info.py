from flask import request

from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.service.ext_table_info import ExtTableInfoService, ExtTableInfoNotExist
from etl.validators import validate_arg
from etl.validators.ext_table_info import (
    GetExtTableInfos,
    CreateExtTableInfo,
    ModifyExtTableInfo,
)
from . import etl_admin_api

service = ExtTableInfoService()


@etl_admin_api.route("/ext_table_infos", methods=["GET"])
@validate_arg(GetExtTableInfos)
def get_ext_table_infos():
    total, ext_table_infos = service.get_ext_table_infos(request.args)
    return jsonify_with_data(
        APIError.OK, data={"total": total, "items": ext_table_infos}
    )


@etl_admin_api.route("/ext_table_infos", methods=["POST"])
@validate_arg(CreateExtTableInfo)
def create_ext_table_info():
    data = request.json
    data["sync_column"] = ",".join(data["sync_column"])
    data["order_column"] = ",".join(data["order_column"])

    service.create_ext_table_info(data)
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_table_info/<int:id>", methods=["GET"])
def get_ext_table_info(id):
    try:
        ext_table_info = service.get_ext_table_info(id)
    except ExtTableInfoNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data=ext_table_info)


@etl_admin_api.route("/ext_table_info/<int:id>", methods=["PATCH"])
@validate_arg(ModifyExtTableInfo)
def modify_ext_table_info(id):
    data = request.json
    if data.get("sync_column") is not None:
        data["sync_column"] = ",".join(data["sync_column"])
    if data.get("order_column") is not None:
        data["order_column"] = ",".join(data["order_column"])
    try:
        service.modify_ext_table_info(id, data)
    except ExtTableInfoNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data={})
