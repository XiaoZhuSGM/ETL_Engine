from flask import request

from etl.service.ext_sql import DatasourceSqlService
from . import etl_admin_api
from .. import jsonify_with_error, jsonify_with_data, APIError

service = DatasourceSqlService()


@etl_admin_api.route("/sql/full", methods=["GET"])
def generate_full_sql():
    source_id = request.args["source_id"]
    extract_date = request.args["date"]
    result = service.generate_full_sql(source_id, extract_date)
    if result:
        return jsonify_with_data(APIError.OK, data=result)
    return jsonify_with_error(APIError.SERVER_ERROR)


@etl_admin_api.route("/sql/tables", methods=["GET"])
def generate_table_sql():
    source_id = request.args["source_id"]
    table_names = request.args["table_names"]
    extract_date = request.args["date"]
    result = service.generate_table_sql(source_id, table_names, extract_date)
    return jsonify_with_data(APIError.OK, data=result)


@etl_admin_api.route("/sql/full/display", methods=["GET"])
def display_full_sql():
    """
    查看根据抓表策略生成的full sql，供人工检查sql的正确性
    :return:
    """
    source_id = request.args["source_id"]
    extract_date = request.args["date"]
    if not all([source_id, extract_date]):
        return jsonify_with_error(APIError.VALIDATE_ERROR)
    result = service.display_full_sql(source_id, extract_date)
    if result:
        return jsonify_with_data(APIError.OK, data=result)
    return jsonify_with_error(APIError.SERVER_ERROR)


@etl_admin_api.route("/sql/target_table", methods=["POST"])
def generate_target_table_sql():
    date = request.get_json()
    source_id = date["source_id"]
    extract_date = date["date"]
    target_table = date["target_table"]
    if not all([source_id, extract_date, target_table]):
        return jsonify_with_error(APIError.VALIDATE_ERROR)
    result = service.generate_target_sql(source_id, extract_date, target_table)
    if result:
        return jsonify_with_data(APIError.OK, data=result)
    return jsonify_with_error(APIError.SERVER_ERROR)


@etl_admin_api.route("/inv/sql", methods=["GET"])
def generate_inv_sql():
    source_id = request.args["source_id"]
    extract_date = request.args["date"]
    result = service.generate_inventory_sql(source_id, extract_date)
    if result:
        return jsonify_with_data(APIError.OK, data=result)
    return jsonify_with_error(APIError.SERVER_ERROR)
