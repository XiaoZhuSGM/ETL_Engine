from . import etl_api
from .. import APIError, jsonify_with_data, jsonify_with_error
from etl.service.iqr import IQRService


@etl_api.route("/iqr/<string:source_id>", methods=["GET"])
def calculate_iqr(source_id):
    iqr_service = IQRService(source_id)
    try:
        result = iqr_service.pipeline()
    except Exception as e:
        print(str(e))
        return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    else:
        return jsonify_with_data(APIError.OK, data=result)
