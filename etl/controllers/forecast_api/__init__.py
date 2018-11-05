from flask import Blueprint

forecast_api = Blueprint("forecast_api", __name__)
from . import hook  # noqa
from . import graph  # noqa
from . import lacking_view  # noqa
from . import store_view  # noqa
