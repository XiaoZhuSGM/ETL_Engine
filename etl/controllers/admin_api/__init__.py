# -*- coding: utf8 -*-

from flask import Blueprint
from flask import jsonify

etl_admin_api = Blueprint('admin_api', __name__)


from .datasource import *  # noqa

from .ext_table_info import *  # noqa


class APIError(object):
    """
    定义状态码
    """

    OK = (200, 'OK')
    NOTFOUBD = (404, 'API or page not found')
    DBCONNECTFALSE = (500, 'ext dadabase connect false')


def jsonify_with_data(err, **kwargs):
    """
    正确相相应返回格式
    :param err:
    :param kwargs:
    :return:
    """
    code, message = err
    meta = {'code': code,
            'message': message}
    return jsonify(meta=meta, **kwargs)


def jsonify_with_error(err, reason=None):
    """
    错误信息返回格式
    :param err:
    :param reason:
    :return:
    """
    code, message = err

    if reason:
        message = '{message}, {reason}'.format(message=message, reason=reason)

    meta = {'code': code,
            'message': message}
    return jsonify(meta=meta, data={})


from . import ext_table

