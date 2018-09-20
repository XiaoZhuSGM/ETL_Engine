# -*- coding: utf8 -*-

from flask import jsonify, Blueprint

from .. import APIError

etl_api = Blueprint('api', __name__)


from . import load_sales_target
from . import hook

@etl_api.errorhandler(404)
def error_404(e):
    """
    Not Found
    :param e:
    :return:
    """

    meta = {'code': 404,
            'message': APIError.NOTFOUBD}
    return jsonify(meta=meta, data={})
