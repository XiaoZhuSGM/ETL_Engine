from . import etl_api
from .. import APIError, jsonify_with_data, jsonify_with_error
from etl.service.load_sales_target import LoadSalestargetServices

services = LoadSalestargetServices()


@etl_api.route("/load/sales_target", methods=["POST"])
def load_sales_target():
    """
    后端调用此接口，导入销售目标表到redshift
    :return:
    """
    try:
        services.load_sales_target()
    except Exception as e:
        print(str(e))
        return jsonify_with_error(APIError.SERVER_ERROR)
    return jsonify_with_data(APIError.OK, data={})

