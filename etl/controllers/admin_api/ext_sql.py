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
