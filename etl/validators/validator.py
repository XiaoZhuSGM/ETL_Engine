# -*- coding: utf-8 -*-

"""
验证前端传来的数据是否符合格式和正确
the valid attributes = ['args', 'form', 'values', 'cookies', 'headers', 'json', 'rule']
"""

from functools import wraps

from flask import request
from wtforms import IntegerField
from wtforms.validators import InputRequired, NumberRange

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


datasource_config_add = {
    "type": "object",
    "properties": {
        "datasource": {
            "type": "object",
            "properties": {
                "source_id": {"type": "string"},
                "cmid": {
                    "type": "array",
                    "items": {
                        "type": "integer",
                    }
                },
                "company_name": {
                    "type": "string",
                },
                "erp_vendor": {
                    "type": "string",
                },
                "db_type": {
                    "type": "string",
                },
                "host": {
                    "type": "string",
                },
                "port": {
                    "type": "integer",
                },
                "username": {
                    "type": "string",
                },
                "password": {
                    "type": "string",
                },
                "db_name": {
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                        },
                        "schema": {
                            "type": "array",
                            "items": {
                                "type": "string",
                            }
                        }
                    }

                },
                "traversal": {
                    "type": "boolean",
                },
                "delta": {
                    "type": "integer",
                },
                "status": {
                    "type": "integer",
                }

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

        },
        "datasource_config": {
            "type": "object",
            "properties": {
                "source_id": {
                    "type": "string",

                },
                "roll_back": {
                    "type": "integer"
                },
                "frequency": {
                    "type": "integer"
                },
                "period": {
                    "type": "integer"
                },
                'ext_time': {
                    'type': 'string'
                }
            },
            'required': [
                'source_id', 'roll_back', 'frequency', 'period', 'ext_time'
            ]
        }
    }
}

datasource_config_update = {
    "type": "object",
    "properties": {
        "datasource": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "source_id": {"type": "string"},
                "cmid": {
                    "type": "array",
                    "items": {
                        "type": "integer",
                    }
                },
                "company_name": {
                    "type": "string",
                },
                "erp_vendor": {
                    "type": "string",
                },
                "db_type": {
                    "type": "string",
                },
                "host": {
                    "type": "string",
                },
                "port": {
                    "type": "integer",
                },
                "username": {
                    "type": "string",
                },
                "password": {
                    "type": "string",
                },
                "db_name": {

                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                        },
                        "schema": {
                            "type": "array",
                            "items": {
                                "type": "string",
                            }
                        }
                    }

                },
                "traversal": {
                    "type": "boolean",
                },
                "delta": {
                    "type": "integer",
                },
                "status": {
                    "type": "integer",
                }
            },
            'required': [
                'id',
                'source_id',
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

        },
        "datasource_config": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "source_id": {
                    "type": "string",
                },
                "roll_back": {
                    "type": "integer"
                },
                "frequency": {
                    "type": "integer"
                },
                "period": {
                    "type": "integer"
                },
                'ext_time': {
                    'type': 'string'
                }
            },
            'required': [
                'source_id', 'roll_back', 'frequency', 'period', 'ext_time'
            ]
        }
    }
}

enterprise_add = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
        },
        "version": {
            "type": "string"
        },
        "remark": {
            "type": "string"
        }
    },
    'required': [
        'name'
    ]
}

enterprise_update = {
    "type": "object",
    "properties": {
        'id': {
            'type': 'integer'
        },
        "name": {
            "type": "string",
        },
        "version": {
            "type": "string"
        },
        "remark": {
            "type": "string"
        }
    },
    'required': [
        'name', 'id'
    ]
}

datasource_test_connection = {
    "type": "object",
    "properties": {
        "db_type": {
            "type": "string",
        },
        "host": {
            "type": "string",
        },
        "port": {
            "type": "integer",
        },
        "username": {
            "type": "string",
        },
        "password": {
            "type": "string",
        },
        "db_name": {
            "type": "object",
            "properties": {
                "database": {
                    "type": "string",
                },
                "schema": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    }
                }
            }

        }
    },
    'required': ['db_type', 'host', 'port', 'username', 'password', 'db_name']
}


class JsonDatasourceTestConnectionInput(Inputs):
    json = [JsonSchema(schema=datasource_test_connection)]


class JsonDatasourceAddInput(Inputs):
    json = [JsonSchema(schema=datasource_config_add)]


class JsonDatasourceUpdateInput(Inputs):
    json = [JsonSchema(schema=datasource_config_update)]


class JsonErpEnterpriseAddInput(Inputs):
    json = [JsonSchema(schema=enterprise_add)]


class JsonErpEnterpriseUpdateInput(Inputs):
    json = [JsonSchema(schema=enterprise_update)]


class PageInput(Inputs):
    args = {
        'page': [InputRequired()]
    }
