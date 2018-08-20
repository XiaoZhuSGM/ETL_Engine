from flask import request
from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.service.ext_clean_info import (
    ExtCleanInfoService,
    ExtCleanInfoParameterError,
    ExtCleanInfoNotFound,
    ExtCleanInfoTableNotFound,
    ExtCleanInfoColumnNotFound,
    ExtTableInfoNotFound,
    ExtDatasourceNotExist,
    TableNotExist
)
from etl.validators import validate_arg
from etl.validators.ext_clean_info import ModiflyExtCleanInfo, CopyExtCleanInfo, GetEXtCleanInfos
from . import etl_admin_api

services = ExtCleanInfoService()


@etl_admin_api.route("/ext_clean_infos", methods=["GET"])
@validate_arg(GetEXtCleanInfos)
def get_ext_clean_infos():
    try:
        total, items = services.get_ext_clean_infos()
    except (ExtCleanInfoParameterError, ExtDatasourceNotExist) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={"total": total, "items": items})


# @etl_admin_api.route("/ext_clean_info", methods=["POST"])
# @validate_arg(CreateExtCleanInfo)
# def create_ext_clean_info():
#     data = request.get_json()
#     try:
#         services.create_ext_clean_info(data)
#     except Exception as e:
#         return jsonify_with_error(APIError.BAD_REQUEST, str(e))
#     return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_clean_info/<int:id>", methods=["PATCH"])
@validate_arg(ModiflyExtCleanInfo)
def modifly_ext_clean_info(id):
    data = request.get_json()
    try:
        services.modifly_ext_clean_info(id, data)
    except (ExtCleanInfoNotFound, ExtCleanInfoTableNotFound, ExtCleanInfoColumnNotFound) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_clean_info/tables/<source_id>", methods=["GET"])
def get_ext_clean_info_table(source_id):
    try:
        tables = services.get_ext_clean_info_table(source_id)
    except ExtTableInfoNotFound as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={"tables": tables})


@etl_admin_api.route("/ext_clean_info/copy", methods=["POST"])
@validate_arg(CopyExtCleanInfo)
def copy_ext_clean_info():
    data = request.get_json()
    try:
        services.copy_ext_clean_info(data)
    except (ExtDatasourceNotExist, TableNotExist) as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data={})
