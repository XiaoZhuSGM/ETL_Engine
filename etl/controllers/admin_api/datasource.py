from flask import request

from . import etl_admin_api
from .. import jsonify_with_error, jsonify_with_data, APIError
from ...service.datasource import DatasourceService
from ...validators.validator import validate_arg, JsonDatasourceAddInput, JsonDatasourceUpdateInput

DATASOURCE_API_CREATE = '/datasource'
DATASOURCE_API_GET = '/datasource/<int:id>'
DATASOURCE_API_GET_ALL = '/datasources'
DATASOURCE_API_UPDATE = '/datasource/<int:id>'

datasourceService = DatasourceService()


@etl_admin_api.route(DATASOURCE_API_CREATE, methods=['POST'])
@validate_arg(JsonDatasourceAddInput)
def add_datasource():
    datasourceJson = request.json
    flag = datasourceService.add_datasource(datasourceJson)
    if flag:
        return jsonify_with_data(APIError.OK)
    else:
        return jsonify_with_error(APIError.SERVER_ERROR)


@etl_admin_api.route(DATASOURCE_API_GET, methods=['GET'])
def get_datasource(id):
    """
    :param id:
    :return:
    """
    datasource = datasourceService.find_datasource_by_id(id)
    if datasource is None:
        return jsonify_with_error(APIError.NOTFOUBD, reason='id don\'t exist')
    else:
        return jsonify_with_data(APIError.OK, data=datasource)


@etl_admin_api.route(DATASOURCE_API_GET_ALL, methods=['GET'])
def get_all_datasource():
    """
    首先需要对page和limit进行判定
    page == None and limit = None get_all
    page < 1 and limit
    :return:
    """
    page = request.args.get('page', default=-1, type=int)
    limit = request.args.get("limit", default=-1, type=int)
    if page == -1 and limit == -1:
        datasource_list = datasourceService.find_all()
        return jsonify_with_data(APIError.OK, data=[datasource.datasourceToDict(datasource) for datasource in datasource_list])
    elif page >= 1 and page >= 1:
        datasource_dict = datasourceService.find_by_page_limit(page, limit)
        print(type(datasource_dict['items']))
        return jsonify_with_data(APIError.OK, data=datasource_dict)
    else:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason='paramter error')


@etl_admin_api.route(DATASOURCE_API_UPDATE, methods=["PATCH"])
@validate_arg(JsonDatasourceUpdateInput)
def update_datasource(id):
    """
    :param id:根据id来进行更新
    :return: update result
    """
    new_datasource_json = request.json
    flag = datasourceService.update_by_id(id, new_datasource_json)
    if flag:
        return jsonify_with_data(APIError.OK)
    else:
        return jsonify_with_data(APIError.SERVER_ERROR)
