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
from etl.validators.ext_clean_info import (
    ModiflyExtCleanInfo,
    CopyExtCleanInfo,
    GetEXtCleanInfos,
    CreateExtCleanInfo
)
from . import etl_admin_api
from etl.constant import PER_PAGE

services = ExtCleanInfoService()


@etl_admin_api.route("/ext_clean_infos", methods=["GET"])
@validate_arg(GetEXtCleanInfos)
def get_ext_clean_infos():
    """
    获得source_id下所有的配置的目标表信息
    :return:
    """
    source_id = request.args.get("source_id")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", PER_PAGE))
    try:
        total, items = services.get_ext_clean_infos(source_id, page, per_page)
    except (ExtCleanInfoParameterError, ExtDatasourceNotExist) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={"total": total, "items": items})


@etl_admin_api.route("/ext_clean_info", methods=["POST"])
@validate_arg(CreateExtCleanInfo)
def create_ext_clean_info():
    """
    新建单个目标表
    :return:
    """
    data = request.get_json()
    try:
        services.create_ext_clean_info(data)
    except ExtDatasourceNotExist as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_clean_info/<int:id>", methods=["DELETE"])
def delete_ext_clean_info(id):
    """
    逻辑删除单个目标表， 该目标表原先的配置不清除
    :param id:
    :return:
    """
    try:
        services.delete_ext_clean_info(id)
    except ExtCleanInfoNotFound as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_clean_info/<int:id>", methods=["PATCH"])
@validate_arg(ModiflyExtCleanInfo)
def modifly_ext_clean_info(id):
    """
    修改单个目标表
    :param id:
    :return:
    """
    data = request.get_json()
    try:
        services.modifly_ext_clean_info(id, data)
    except (ExtCleanInfoNotFound, ExtCleanInfoTableNotFound, ExtCleanInfoColumnNotFound) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_clean_info/target_table/<source_id>", methods=["GET"])
def get_ext_clean_info_target_table(source_id):
    """
    获得还未添加的目标表，用于新增单个目标表的下拉选项
    :param source_id:
    :return:
    """
    tables = services.get_ext_clean_info_target_table(source_id)
    return jsonify_with_data(APIError.OK, data={"tables": tables})


@etl_admin_api.route("/ext_clean_info/tables/<source_id>", methods=["GET"])
def get_ext_clean_info_table(source_id):
    """
    获取该source_id对应下的配置为抓取的表，用于选择原始表的下拉选项
    :param source_id:
    :return:
    """
    try:
        tables = services.get_ext_clean_info_table(source_id)
    except ExtTableInfoNotFound as e:
        return jsonify_with_error(APIError.BAD_REQUEST, str(e))
    return jsonify_with_data(APIError.OK, data={"tables": tables})


@etl_admin_api.route("/ext_clean_info/copy", methods=["POST"])
@validate_arg(CopyExtCleanInfo)
def copy_ext_clean_info():
    """
    将某个source_id的目标表配置copy复制到另一个source_id下，用于同个erp类型
    :return:
    """
    data = request.get_json()
    try:
        services.copy_ext_clean_info(data)
    except (ExtDatasourceNotExist, TableNotExist) as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_clean_info/target", methods=["GET"])
def get_ext_clean_info_target():
    """
    获取source_id下的某个目标表的信息
    :return:
    """
    source_id = request.args.get("source_id")
    target = request.args.get("target")
    try:
        data = services.get_ext_clean_info_target(source_id, target)
    except (ExtCleanInfoParameterError, ExtCleanInfoNotFound) as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data={"target": data})
