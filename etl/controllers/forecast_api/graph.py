from . import forecast_api
from .. import APIError, jsonify_with_data, jsonify_with_error
from etl.service.forecast import (
    ForecastService,
    ForecastError,
    store_hash,
    r_store_hash,
    enterprise_hash,
)
from flask import request

forecast_service = ForecastService()


@forecast_api.route("/graph/authorize", methods=["POST"])
def authorize():
    command = request.json.get("command")
    if command in store_hash:
        store = store_hash[command]
        data = {
            "type": "store",
            "info": [{"command": command, "store": store["store_name"]}],
        }
        return jsonify_with_data(APIError.OK, data=data)
    elif command in enterprise_hash:
        stores = r_store_hash[enterprise_hash[command]]
        data = {
            "type": "enterprise",
            "info": [
                {"store": v["store_name"], "command": k} for k, v in stores.items()
            ],
        }
        return jsonify_with_data(APIError.OK, data=data)
    else:
        return jsonify_with_error(APIError.UNAUTHORIZED)


@forecast_api.route("/graph/lacking", methods=["GET"])
def lacking():
    command = request.args.get("command")
    try:
        store_info = forecast_service.login(command)
    except ForecastError as e:
        return jsonify_with_error(APIError.UNAUTHORIZED, reason=e)
    data = forecast_service.lacking_rate(store_info["cmid"], store_info["store_id"])
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/graph/best_lacking", methods=["GET"])
def best_lacking():
    command = request.args.get("command")
    try:
        store_info = forecast_service.login(command)
    except ForecastError as e:
        return jsonify_with_error(APIError.UNAUTHORIZED, reason=e)
    data = forecast_service.best_lacking(store_info["cmid"], store_info["store_id"])
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/graph/performance_process", methods=["GET"])
def performance_process():
    command = request.args.get("command")
    try:
        store_info = forecast_service.login(command)
    except ForecastError as e:
        return jsonify_with_error(APIError.UNAUTHORIZED, reason=e)
    data = forecast_service.performance_process(store_info["cmid"])
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/graph/order_rate", methods=["GET"])
def order_rate():
    command = request.args.get("command")
    try:
        store_info = forecast_service.login(command)
    except ForecastError as e:
        return jsonify_with_error(APIError.UNAUTHORIZED, reason=e)
    data = forecast_service.order_rate(store_info["cmid"], store_info["store_id"])
    return jsonify_with_data(APIError.OK, data=data)
