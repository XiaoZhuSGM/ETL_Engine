from . import etl_api
from .. import APIError, jsonify_with_data, jsonify_with_error
from etl.service.iqr import IQRService
from etl.extensions import cache
from datetime import datetime
import pytz

_TZ = pytz.timezone("Asia/Shanghai")


@etl_api.route("/iqr/<string:source_id>", methods=["GET"])
def calculate_iqr(source_id):
    iqr_service = IQRService(source_id)
    cache_key = f"iqr_{source_id}"
    try:
        hour = datetime.now(_TZ).hour
        result = cache.get(cache_key)
        print(result)
        if hour == 4 or (result is None):
            print("进行计算了")
            result = iqr_service.pipeline()
            cache.set(cache_key, result)
    except Exception as e:
        print(str(e))
        return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    else:
        return jsonify_with_data(APIError.OK, data=result)
