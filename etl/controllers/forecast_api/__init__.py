from flask import Blueprint

forecast_api = Blueprint("forecast_api", __name__)
from . import param_platform
from . import suggest
