# -*- coding: utf-8 -*-
# @Time    : 2018/8/14 下午2:14
# @Author  : 范佳楠

from flask import request

from . import etl_admin_api
from .. import jsonify_with_error, jsonify_with_data, APIError
from ...service.erp_enterprise import ErpEnterpriseService, ErpEnterpriseNotExist
from ...validators.validator import validate_arg, JsonErpEnterpriseAddInput, JsonErpEnterpriseUpdateInput


ENTERPRISE_API_CREATE = '/enterprise'
ENTERPRISE_API_GET = '/enterprise/<int:enterprise_id>'
ENTERPRISE_API_GET_ALL = '/enterprises'
ENTERPRISE_API_UPDATE = '/enterprise/<int:enterprise_id>'

enterprise_service = ErpEnterpriseService()


@etl_admin_api.route(ENTERPRISE_API_CREATE, methods=['POST'])
@validate_arg(JsonErpEnterpriseAddInput)
def add_enterprise():
    enterprise_json = request.json
    flag = enterprise_service.add_enterprise(enterprise_json)
    if flag:
        return jsonify_with_data(APIError.OK)
    else:
        return jsonify_with_error(APIError.SERVER_ERROR)


@etl_admin_api.route(ENTERPRISE_API_GET, methods=['GET'])
def get_enterprise(enterprise_id):
    try:
        enterprise = enterprise_service.get_enterprise_by_id(enterprise_id)
        return jsonify_with_data(APIError.OK, data=enterprise)
    except ErpEnterpriseNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, reason=str(e))


@etl_admin_api.route(ENTERPRISE_API_UPDATE, methods=['PATCH'])
@validate_arg(JsonErpEnterpriseUpdateInput)
def update_enterprise(enterprise_id):
    try:
        new_enterprise_json = request.json
        enterprise_service.update_enterprise(enterprise_id, new_enterprise_json)
        return jsonify_with_data(APIError.OK)
    except ErpEnterpriseNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, reason=str(e))


@etl_admin_api.route(ENTERPRISE_API_GET_ALL, methods=['GET'])
def get_all_enterprise():
    page = request.args.get('page', default=-1, type=int)
    per_page = request.args.get("per_page", default=-1, type=int)
    if page == -1 and per_page == -1:
        enterprise_list = enterprise_service.find_all()
        return jsonify_with_data(APIError.OK, data=[enterprise.to_dict() for enterprise in enterprise_list])
    elif page >= 1 and per_page >= 1:
        enterprise_dict = enterprise_service.find_by_page_limit(page, per_page)
        return jsonify_with_data(APIError.OK, data=enterprise_dict)
    else:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason='paramter error')
