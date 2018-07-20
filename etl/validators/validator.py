# -*- coding: utf-8 -*-

"""
验证前端传来的数据是否符合格式和正确
the valid attributes = ['args', 'form', 'values', 'cookies', 'headers', 'json', 'rule']
"""

from functools import wraps

from flask import request
from wtforms import StringField
from wtforms.validators import Email
from wtforms.validators import InputRequired
from wtforms.validators import Regexp

from etl.controllers import APIError, jsonify_with_error
from flask_inputs import Inputs
from flask_inputs.validators import JsonSchema

RE_MOBILE = '^1[0-9]{10}$'

RE_TEL = '[0-9]+'


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


class SmsRegisterInput(Inputs):
    form = {
        'mobile': (StringField, [InputRequired(), Regexp(RE_MOBILE)]),
        'code': (StringField, [InputRequired()]),
    }


class SendVerificationInput(Inputs):
    form = {
        'mobile': (StringField, [InputRequired(), Regexp(RE_MOBILE)]),
        'access_token': (StringField, [InputRequired()]),
    }


class MobileExistInput(Inputs):
    form = {
        'mobile': (StringField, [InputRequired(), Regexp(RE_MOBILE)]),
    }


class EmailExistInput(Inputs):
    form = {
        'email': (StringField, [InputRequired(), Email()]),
    }


login_schema = {
    'type': 'object',
    'properties': {
        'mobile': {'pattern': RE_MOBILE},
        'user_name': {'type': 'string'},
        'password': {'type': 'string'}
    },
    'required': ['mobile', 'password']
}

teacher_add = {
    'type': 'object',
    'properties': {
        'mobile': {'pattern': RE_MOBILE},
        'name': {'type': 'string'},
        'belonged_school_id': {'type': 'integer'}
    },
    'required': ['mobile', 'name', 'belonged_school_id']
}

school_add = {
    'type': 'object',
    'properties': {
        'tel': {'pattern': RE_TEL},
        'name': {'type': 'string'},
        'address': {'type': 'string'},
    },
    'required': ['tel', 'name', 'address']
}

datasource_add = {
    'type': 'object',
    'properties': {
        'source_id': {'type': 'string'},
        'cmid': {'type': 'string'},
        'company_name': {'type': 'string'},
        'erp_vendor': {'type': 'string'},
        'dp_type': {'type': 'string'},
        'host': {'type': 'string'},
        'port': {'type': 'integer'},
        'username': {'type': 'string'},
        'password': {'type': 'string'},
        'db_schema': {'type': 'string'},
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
                 'db_schema',
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
        'cmid': {'type': 'string'},
        'company_name': {'type': 'string'},
        'erp_vendor': {'type': 'string'},
        'dp_type': {'type': 'string'},
        'host': {'type': 'string'},
        'port': {'type': 'integer'},
        'username': {'type': 'string'},
        'password': {'type': 'string'},
        'db_schema': {'type': 'string'},
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
                 'password',
                 'db_schema',
                 'db_name',
                 'traversal',
                 'delta',
                 'status']
}


class JsonDatasourceAddInput(Inputs):
    json = [JsonSchema(schema=datasource_add)]


class JsonDatasourceUpdateInput(Inputs):
    json = [JsonSchema(schema=datasource_update)]


class JsonSchoolInput(Inputs):  # 验证JSON
    json = [JsonSchema(schema=school_add)]


class JsonLoginInput(Inputs):
    json = [JsonSchema(schema=login_schema)]


class JsonTeacherInput(Inputs):
    json = [JsonSchema(schema=teacher_add)]
