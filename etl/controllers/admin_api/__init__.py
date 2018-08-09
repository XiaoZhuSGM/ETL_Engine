from flask import Blueprint
from flask import jsonify

etl_admin_api = Blueprint('admin_api', __name__)


@etl_admin_api.route("/ping")
def ping():
    return jsonify({"ping": "pong"})

from .hook import *  # noqa

from .datasource import *  # noqa

from .ext_table_info import *  # noqa

from .ext_datasource_con import *  # noqa

from .login import *  # noqa

from .ext_table import *  # noqa
