from . import forecast_api
from .. import APIError, jsonify_with_data
from etl.service.forecast import BossService
from flask import request

boss_service = BossService()


@forecast_api.route("/store_view/goods")
def store_view_goods():
    command = request.args.get("command")
    query = request.args.get("query")
    boss_info = boss_service.login(command)
    data = boss_service.goods(boss_info["cmid"], query)
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/store_view/goods/<item_id>")
def store_view_goods_detail(item_id):
    command = request.args.get("command")
    boss_info = boss_service.login(command)
    data = boss_service.goods_detail(boss_info["cmid"], item_id)
    return jsonify_with_data(APIError.OK, data=data)


@forecast_api.route("/store_view/goods/<item_id>/<store_id>")
def store_view_store_goods_detail(item_id, store_id):
    command = request.args.get("command")
    boss_info = boss_service.login(command)
    data = boss_service.store_goods_detail(boss_info["cmid"], item_id, store_id)
    return jsonify_with_data(APIError.OK, data=data)
