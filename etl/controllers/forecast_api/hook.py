from . import forecast_api
from .. import APIError, jsonify_with_error
from flask import request
from etl.service.forecast import ForecastError, ForecastService, BossService

forecast_service = ForecastService()
boss_service = BossService()


@forecast_api.before_request
def before_request():
    if request.endpoint in {"forecast_api.authorize"}:
        return
    command = request.args.get("command")
    try:
        if request.endpoint.startswith("forecast_api.lacking_view"):
            boss_service.login(command)
        else:
            forecast_service.login(command)
    except ForecastError as e:
        return jsonify_with_error(APIError.UNAUTHORIZED, reason=e)
