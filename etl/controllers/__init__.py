from flask import jsonify


class APIError(object):
    """
    定义状态码
    """

    OK = (200, 'OK')
    NOTFOUND = (404, 'API or page not found')
    VALIDATE_ERROR = (417, 'paramter error')
    SERVER_ERROR = (503, 'server error')
    BAD_REQUEST = (400, 'database connect fail')


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
