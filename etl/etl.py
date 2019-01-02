from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from raven.contrib.flask import Sentry
from etl.extensions import cache
from etl.flask_celery import Celery
from redis import StrictRedis


__all__ = ["create_app"]

DEFAULT_APP_NAME = "etl"
db = SQLAlchemy()
celery = Celery()
sentry = Sentry()


def create_app(config=None):
    app = Flask(DEFAULT_APP_NAME, instance_relative_config=True)
    if config is not None:
        app.config.from_object(config)
    app.config.from_pyfile("local_config.py", silent=True)  # 加载个人配置
    db.init_app(app)

    configure_celery(app)
    configure_path_converter(app)
    configure_blueprints(app)
    configure_sentry(app)
    configure_extensions(app)

    CORS(
        app,
        resources={
            r"/etl/admin/api/*": {"origins": "*"},
            r"/etl/api/*": {"origins": "*"},
            r"/forecast/api/*": {"origins": "*"},
        },
    )

    return app


def configure_celery(app):
    celery.init_app(app)


def configure_sentry(app):
    sentry.init_app(app, dsn=app.config["SENTRY_DSN"])


def configure_path_converter(app):
    from werkzeug.routing import PathConverter

    class EverythingConverter(PathConverter):
        regex = ".*?"

    app.url_map.converters["everything"] = EverythingConverter


def configure_blueprints(app):
    from etl.controllers.api import etl_api as api
    from etl.controllers.admin_api import etl_admin_api as admin_api
    from etl.controllers.forecast_api import forecast_api

    app.register_blueprint(api, url_prefix="/etl/api")
    app.register_blueprint(admin_api, url_prefix="/etl/admin/api")
    app.register_blueprint(forecast_api, url_prefix="/forecast/api")


def configure_extensions(app):
    cache.init_app(app)
