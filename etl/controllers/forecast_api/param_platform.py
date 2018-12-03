from flask import request, send_from_directory

from . import forecast_api
from .. import APIError, jsonify_with_data, jsonify_with_error

from etl.service.display_info import DisplayInfo, DisplayInfoExist, ForeignStoreIdNotExist
from etl.service.delivery_period import DeliveryPeriodService, DeliveryPeriodExist, DeliveryPeriodNotExist

import os

display_info = DisplayInfo()
delivery_period = DeliveryPeriodService()


@forecast_api.route("/param/info")
def display_homepage():
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)
    cmid_list = display_info.get_cmid()
    data = display_info.find_by_page_limit(page, per_page, 79, '1000044')
    if data:
        return jsonify_with_data(
            APIError.OK,
            data={'cmid_list': cmid_list,
                  'store_data': data.get('items'),
                  'total_page': data.get('total_page'),
                  'cur_page': data.get('cur_page'),
                  }
        )
    else:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason="paramter error")


@forecast_api.route("/param/info/<cmid>/<store_id>")
def get_display_info(cmid, store_id):
    try:
        display_info.get_info_from_store_id(cmid, store_id)
    except ForeignStoreIdNotExist as e:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))

    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)
    store_data = display_info.find_by_page_limit(page, per_page, cmid, store_id)
    return jsonify_with_data(APIError.OK,
                             data={
                                 'store_data': store_data.get('items'),
                                 'total_page': store_data.get('total_page'),
                                 'cur_page': store_data.get('cur_page'),
                             })


@forecast_api.route("/param/add", methods=["POST"])
def add_display_info():
    data = request.json
    try:
        display_info.create(**data)
    except DisplayInfoExist as e:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))

    return jsonify_with_data(APIError.OK)


@forecast_api.route("/param/delete", methods=['POST'])
def delete_display_info():
    data = request.json
    id_list = data.get('id')
    if isinstance(id_list, int):
        id_list = [id_list]
    display_info.delete_info(id_list)

    return jsonify_with_data(APIError.OK)


@forecast_api.route("/param/update", methods=['POST'])
def update_display_info():
    data = request.json
    display_info.update_info(**data)

    return jsonify_with_data(APIError.OK)


@forecast_api.route("/param/upload_file", methods=['POST'])
def param_upload_file():
    if 'file' not in request.files:
        return jsonify_with_error(APIError.VALIDATE_ERROR, 'file failed')
    file = request.files['file']
    if file and display_info.allowed_file(file.filename):
        display_info.process_file(file)
        return jsonify_with_data(APIError.OK)

    return jsonify_with_error(APIError.VALIDATE_ERROR, '文件格式有误')


@forecast_api.route("/param/download/<path:filename>")
def param_download(filename):
    dirpath = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../service'))
    return send_from_directory(dirpath, filename, as_attachment=True)


@forecast_api.route("/param/delivery")
def delivery():
    page = request.args.get("page", default=-1, type=int)
    per_page = request.args.get("per_page", default=-1, type=int)
    cmid_list = delivery_period.get_cmid()
    result = delivery_period.find_by_page_limit(page, per_page, 43)
    if not result:
        result = []
    return jsonify_with_data(APIError.OK,
                             data={'cmid_list': cmid_list,
                                   'result': result.get('items'),
                                   'cur_page': result.get('cur_page'),
                                   'total_page': result.get('total_page'),
                                   }
                             )


@forecast_api.route("/param/delivery/<cmid>", methods=['POST', 'GET'])
def get_delivery_from_cmid(cmid):
    if request.method == 'POST':
        forieign_store_id = request.json.get('foreign_store_id')
        if forieign_store_id:
            try:
                result = []
                data = delivery_period.get_info_from_store_id(cmid, forieign_store_id)
                result.append(data)
            except DeliveryPeriodNotExist as e:
                return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))
            return jsonify_with_data(APIError.OK, data={"result": result})
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason='Key Value')

    result = delivery_period.get_store_id(cmid)
    return jsonify_with_data(APIError.OK, data={"result": result})


@forecast_api.route("/param/delivery/add", methods=['POST'])
def delivery_add():
    data = request.json
    try:
        delivery_period.create(**data)
    except DeliveryPeriodExist as e:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))

    return jsonify_with_data(APIError.OK)


@forecast_api.route("/param/delivery/delete", methods=['POST'])
def delivery_delete():
    data = request.json
    id_list = data.get("id")
    if isinstance(id_list, int):
        id_list = [id_list]

    delivery_period.delete(id_list)
    return jsonify_with_data(APIError.OK)


@forecast_api.route("/param/delivery/update", methods=['POST'])
def update_delievry():
    data = request.json
    delivery_period.update_info(**data)

    return jsonify_with_data(APIError.OK)
