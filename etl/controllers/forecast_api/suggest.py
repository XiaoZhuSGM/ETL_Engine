from flask import request

from etl.service.suggest_order import SuggestOrderService, SuggestOrderNotExist, SuggestOrderItemExist
from .. import jsonify_with_data, APIError, jsonify_with_error
from etl.controllers.forecast_api import forecast_api

suggest_order = SuggestOrderService()


@forecast_api.route("/suggest/info", methods=['GET'])
def get_suggest_cmid():
    cmid = suggest_order.show_cmid()
    store_id = suggest_order.show_store_id(43)
    data_source = suggest_order.show_info_list(43, 1000095)
    data = [
        {'cmid': 43,
         'store_id': 1000095,
         'foreign_item_id': item[0],
         'fill_qty': item[1],
         'show_code': item[2],
         'source': '算法预测',
         'action': ''} for item in data_source
    ]
    return jsonify_with_data(APIError.OK, data={'store_id': store_id, 'cmid': cmid, 'data': data})


@forecast_api.route("/suggest/info/<cmid>", methods=['GET'])
def get_suggest_store(cmid):
    store_id = suggest_order.show_store_id(cmid)
    return jsonify_with_data(APIError.OK, data={'store_id': store_id})


@forecast_api.route("/suggest/info/<cmid>/<store_id>", methods=['GET'])
def get_suggest_info(cmid, store_id):
    try:
        data_source = suggest_order.show_info_list(cmid, store_id)
    except SuggestOrderNotExist as e:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))
    data = [
        {'cmid': cmid,
         'store_id': store_id,
         'foreign_item_id': item[0],
         'fill_qty': item[1],
         'show_code': item[2],
         'source': '算法预测',
         'action': ''} for item in data_source
    ]

    return jsonify_with_data(APIError.OK, data={'data': data})


@forecast_api.route("/suggest/add", methods=['POST'])
def get_suggest_add():
    data = request.json
    try:
        dataframe = suggest_order.suggest_order_add(**data)
    except SuggestOrderItemExist as e:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))
    suggest_order.upload_to_s3(dataframe, **data)

    return jsonify_with_data(APIError.OK)


@forecast_api.route("/suggest/remove", methods=['POST'])
def get_suggest_remove():
    data = request.json
    dataframe = suggest_order.suggest_order_remove(**data)
    suggest_order.upload_to_s3(dataframe, **data)

    return jsonify_with_data(APIError.OK)


@forecast_api.route("/suggest/update", methods=['POST'])
def get_suggest_update():
    data = request.json
    dataframe = suggest_order.suggest_order_update(**data)
    suggest_order.upload_to_s3(dataframe, **data)

    return jsonify_with_data(APIError.OK)
