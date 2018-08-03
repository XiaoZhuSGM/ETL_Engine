# -*- coding: utf-8 -*-

"""
验证前端传来的数据是否符合格式和正确
the valid attributes = ['args', 'form', 'values', 'cookies', 'headers', 'json', 'rule']
"""

from functools import wraps

from flask import request

from etl.controllers import APIError, jsonify_with_error
from flask_inputs import Inputs
from flask_inputs.validators import JsonSchema


def validate_arg(validator):
    """
    验证字段合法装饰器
    :param validator:
    :return:
    """

    def deco(f):
        @wraps(f)
        def _(*args, **kwargs):
            validate_result = validator(request)
            if not validate_result.validate():
                # return jsonify_with_error(APIError.VALIDATE_ERROR, reason=json.dumps(request.json))
                return jsonify_with_error(APIError.VALIDATE_ERROR, reason=validate_result.errors[0])
            resp = f(*args, **kwargs)
            return resp

        return _

    return deco


datasource_add = {
    'type': 'object',
    'properties': {
        'source_id': {'type': 'string'},
        'cmid': {'type': 'array', 'items': {'type': 'number'}},
        'company_name': {'type': 'string'},
        'erp_vendor': {'type': 'string'},
        'dp_type': {'type': 'string'},
        'host': {'type': 'string'},
        'port': {'type': 'integer'},
        'username': {'type': 'string'},
        'password': {'type': 'string'},
        'db_name': {'type': 'array'},
        'traversal': {'type': 'boolean'},
        'delta': {'type': 'integer'},
        'status': {'type': 'integer'},

    },
    'required': ['source_id',
                 'cmid',
                 'company_name',
                 'erp_vendor',
                 'db_type',
                 'host',
                 'port',
                 'username',
                 'password',
                 'db_name',
                 'traversal',
                 'delta',
                 'status']
}

datasource_update = {
    'type': 'object',
    'properties': {
        'id': {'type': 'integer'},
        'source_id': {'type': 'string'},
        'cmid': {'type': 'array', 'items': {'type': 'number'}},
        'company_name': {'type': 'string'},
        'erp_vendor': {'type': 'string'},
        'dp_type': {'type': 'string'},
        'host': {'type': 'string'},
        'port': {'type': 'integer'},
        'username': {'type': 'string'},
        'password': {'type': 'string'},
        'db_name': {'type': 'array'},
        'traversal': {'type': 'boolean'},
        'delta': {'type': 'integer'},
        'status': {'type': 'integer'},

    },
    'required': ['id',
                 'source_id',
                 'cmid',
                 'company_name',
                 'erp_vendor',
                 'db_type',
                 'host',
                 'port',
                 'username',
                 'db_name',
                 'traversal',
                 'delta',
                 'status']
}


class JsonDatasourceAddInput(Inputs):
    json = [JsonSchema(schema=datasource_add)]


class JsonDatasourceUpdateInput(Inputs):
    json = [JsonSchema(schema=datasource_update)]
