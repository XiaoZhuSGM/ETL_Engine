from . import forecast_api
from .. import APIError, jsonify_with_data, jsonify_with_error
from etl.service.forecast import boss_hash, r_store_hash, BossService
from flask import request

boss_service = BossService()


@forecast_api.route("/lacking_view/authorize", methods=["POST"])
def lacking_view_authorize():
    command = request.json.get("command")
    if command in boss_hash:
        stores = r_store_hash[boss_hash[command]]
        data = {
            "type": "boss",
            "info": [
                {"store": v["store_name"], "command": v["command"]}
                for v in stores.values()
            ],
        }
        return jsonify_with_data(APIError.OK, data=data)
    else:
        return jsonify_with_error(APIError.UNAUTHORIZED)


@forecast_api.route("/lacking_view/lack_rate")
def lacking_view_lack_rate():
    command = request.args.get("command")
    boss_info = boss_service.login(command)
    data = boss_service.lacking_rate(boss_info["cmid"])
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/lacking_view/lost_sales")
def lacking_view_lost_sales():
    command = request.args.get("command")
    boss_info = boss_service.login(command)
    data = boss_service.lost_sales(boss_info["cmid"])
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/lacking_view/best_lacking")
def lacking_view_best_lacking():
    command = request.args.get("command")
    boss_info = boss_service.login(command)
    data = boss_service.best_lacking(boss_info["cmid"])
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/lacking_view/stores")
def lacking_view_stores():
    command = request.args.get("command")
    boss_info = boss_service.login(command)
    data = boss_service.stores(boss_info["cmid"])
    return jsonify_with_data(APIError.OK, data=data)
