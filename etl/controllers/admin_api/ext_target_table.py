from etl.service.ext_target_table import TargetTableService
from . import etl_admin_api
from .. import jsonify_with_data, APIError

service = TargetTableService()


@etl_admin_api.route("/target_table_info", methods=["GET"])
def get_target_table_info():
    service.store_target_table_info()
    return jsonify_with_data(APIError.OK, data={})
