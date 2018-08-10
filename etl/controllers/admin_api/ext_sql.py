from flask import request
from . import etl_admin_api
from etl.validators import validate_arg
from etl.service.ext_sql import DatasourceSqlService
from .. import jsonify_with_error, jsonify_with_data, APIError

service = DatasourceSqlService()


@etl_admin_api.route("/sql/generate", methods=['GET'])
def generate_sql():
    source_id = request.args["source_id"]
    extract_date = request.args["date"]
    result = service.genereate_for_full_sql(source_id, extract_date)
    if result:
        return jsonify_with_data(APIError.OK, data=result)
    return jsonify_with_error(APIError.SERVER_ERROR)
