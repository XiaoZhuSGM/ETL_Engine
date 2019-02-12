from . import etl_api
from .. import APIError, jsonify_with_data, jsonify_with_error
import pytz
from etl.tasks.tasks import task_iqr
from flask import request

_TZ = pytz.timezone("Asia/Shanghai")


@etl_api.route("/task/iqr", methods=["POST"])
def trigger_task_iqr():

    # iqr_service = IQRService(source_id)
    # cache_key = f"iqr_{source_id}"
    # try:
    #     hour = datetime.now(_TZ).hour
    #     result = cache.get(cache_key)
    #     print(result)
    #     if hour == 4 or (result is None):
    #         print("进行计算了")
    #         result = iqr_service.pipeline()
    #         cache.set(cache_key, result)
    # except Exception as e:
    #     print(str(e))
    #     return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    # else:
    #     return jsonify_with_data(APIError.OK, data=result)
    message = request.json
    result = task_iqr.apply_async(kwargs={"source_id": message["source_id"]})
    return jsonify_with_data(APIError.OK, data={"task_id": result.id})


@etl_api.route("/task/iqr/status", methods=["GET"])
def get_task_iqr_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = False
    async_result = task_iqr.AsyncResult(task_id=task_id)

    if async_result.successful():
        result = async_result.result
        status = "success"
    elif async_result.failed():
        status = "failed"
        reason = str(async_result.info)
    else:
        status = "running"
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )

