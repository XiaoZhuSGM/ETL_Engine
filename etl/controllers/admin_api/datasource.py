from flask import request

from . import etl_admin_api
from .. import jsonify_with_error, jsonify_with_data, APIError
from ...service.datasource import DatasourceService
from ...service.datasource import ExtDatasourceNotExist, ExtDatasourceConfigNotExist
from ...service.ext_table import (
    ExtTableService,
    ExtDatasourceParaMiss,
    ExtDatasourceConnError,
)
from ...validators.validator import (
    validate_arg,
    JsonDatasourceAddInput,
    JsonDatasourceUpdateInput,
    JsonDatasourceTestConnectionInput,
)

DATASOURCE_API_CREATE = "/datasource"
DATASOURCE_API_GET = "/datasource/<string:source_id>"
DATASOURCE_API_GET_ALL = "/datasources"
DATASOURCE_API_UPDATE = "/datasource/<int:datasource_id>"
DATASOURCE_API_TEST = "/datasource/test"
DATASOURCE_API_GET_BY_ERP = "/datasource/erp/<string:erp_vendor>"

DATASOURCE_API_GET_BY_STATUS = "/datasource/status/<int:status>"

DATASOURCE_API_GET_BY_FILTER = "/datasource/filter"

DATASOURCE_API_GENERATOR_CRON = "/crontab/full/<string:source_id>"
DATASOURCE_API_GENERATOR_EXTRACT_EVENT = "/extract/event/<string:source_id>"
DATASOURCE_API_GET_ALL_ONLINE_ENTERPRISE = "/datasources/online"

datasource_service = DatasourceService()
table_service = ExtTableService()


@etl_admin_api.route(DATASOURCE_API_GET_BY_FILTER, methods=["GET"])
def get_datasource_by_filter():
    args = request.args
    datasource_list = datasource_service.find_datasource_by_filter(args)
    return jsonify_with_data(
        APIError.OK,
        data=[datasource.to_dict_and_config() for datasource in datasource_list],
    )


@etl_admin_api.route(DATASOURCE_API_GET_BY_STATUS, methods=["GET"])
def get_datasource_by_status(status):
    datasource_list = datasource_service.find_datasource_by_status(status)
    return jsonify_with_data(
        APIError.OK,
        data=[datasource.to_dict_and_config() for datasource in datasource_list],
    )


@etl_admin_api.route(DATASOURCE_API_GET_ALL_ONLINE_ENTERPRISE, methods=["GET"])
def generate_online_enterprice():
    enterprices = datasource_service.generate_online_enterprice()
    return jsonify_with_data(APIError.OK, data=enterprices)


@etl_admin_api.route(DATASOURCE_API_GENERATOR_EXTRACT_EVENT, methods=["GET"])
def generate_extract_event(source_id):
    try:
        event = datasource_service.generator_extract_event(source_id)
        return jsonify_with_data(APIError.OK, data=event)
    except ExtDatasourceNotExist:
        return jsonify_with_error(APIError.NOTFOUND)


@etl_admin_api.route(DATASOURCE_API_GENERATOR_CRON, methods=["GET"])
def generate_crontab(source_id):
    cron_expression = datasource_service.generator_full_crontab_expression(source_id)
    if cron_expression:
        return jsonify_with_data(APIError.OK, data=cron_expression)
    else:
        return jsonify_with_error(APIError.NOTFOUND)


@etl_admin_api.route(DATASOURCE_API_CREATE, methods=["POST"])
@validate_arg(JsonDatasourceAddInput)
def add_datasource():
    datasource_and_config_json = request.json
    flag = datasource_service.add_datasource(datasource_and_config_json)
    if flag:
        return jsonify_with_data(APIError.OK)
    else:
        return jsonify_with_error(APIError.SERVER_ERROR)


@etl_admin_api.route(DATASOURCE_API_GET, methods=["GET"])
def get_datasource(source_id):
    try:
        datasource = datasource_service.find_datasource_by_source_id(source_id)
        return jsonify_with_data(APIError.OK, data=datasource)
    except ExtDatasourceNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, reason=str(e))
    except ExtDatasourceConfigNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, reason=str(e))


@etl_admin_api.route(DATASOURCE_API_GET_ALL, methods=["GET"])
def get_all_datasource():
    page = request.args.get("page", default=-1, type=int)
    per_page = request.args.get("per_page", default=-1, type=int)
    if page == -1 and per_page == -1:
        datasource_list = datasource_service.find_all()
        return jsonify_with_data(
            APIError.OK,
            data=[datasource.to_dict_and_config() for datasource in datasource_list],
        )
    elif page >= 1 and per_page >= 1:
        datasource_dict = datasource_service.find_by_page_limit(page, per_page)
        return jsonify_with_data(APIError.OK, data=datasource_dict)
    else:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason="paramter error")


@etl_admin_api.route(DATASOURCE_API_UPDATE, methods=["PATCH"])
@validate_arg(JsonDatasourceUpdateInput)
def update_datasource(datasource_id):
    new_datasource_and_config_json = request.json
    try:
        datasource_service.update_by_id(datasource_id, new_datasource_and_config_json)
        return jsonify_with_data(APIError.OK)
    except ExtDatasourceNotExist as e:
        return jsonify_with_data(APIError.SERVER_ERROR, reason=str(e))
    except ExtDatasourceConfigNotExist as e:
        return jsonify_with_data(APIError.SERVER_ERROR, reason=str(e))


@etl_admin_api.route(DATASOURCE_API_TEST, methods=["POST"])
@validate_arg(JsonDatasourceTestConnectionInput)
def test_connection_datasource():
    data = request.json
    try:
        table_service.connect_test(data)
    except (ExtDatasourceConnError, ExtDatasourceParaMiss) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, reason=str(e))
    return jsonify_with_data(APIError.OK)


@etl_admin_api.route(DATASOURCE_API_GET_BY_ERP, methods=["GET"])
def get_datasouce_by_erp(erp_vendor):
    datasource_list = datasource_service.find_datasource_by_erp(erp_vendor)
    return jsonify_with_data(
        APIError.OK,
        data=[datasource.to_dict_and_config() for datasource in datasource_list],
    )
