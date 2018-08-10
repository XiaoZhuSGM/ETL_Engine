from celery import Celery
from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from raven.contrib.flask import Sentry

__all__ = ['create_app']

DEFAULT_APP_NAME = 'etl'
db = SQLAlchemy()
celery = Celery(DEFAULT_APP_NAME)
sentry = Sentry()


def create_app(config=None):
    app = Flask(DEFAULT_APP_NAME, instance_relative_config=True)
    if config is not None:
        app.config.from_object(config)
    # app.config.from_pyfile('local_config.py', silent=True)  # 加载个人配置
    db.init_app(app)

    configure_celery(app)

    configure_path_converter(app)
    configure_blueprints(app)
    configure_sentry(app)

    CORS(app, resources={r"/etl/admin/api/*": {"origins": "*"}})
    CORS(app, resources={r"/etl/api/*": {"origins": "*"}})

    return app


def configure_celery(app):
    celery.config_from_object(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask


def configure_sentry(app):
    sentry.init_app(app, dsn=app.config["SENTRY_DSN"])


def configure_path_converter(app):
    from werkzeug.routing import PathConverter

    class EverythingConverter(PathConverter):
        regex = '.*?'

    app.url_map.converters['everything'] = EverythingConverter


def configure_blueprints(app):
    from etl.controllers.api import etl_api as api
    from etl.controllers.admin_api import etl_admin_api as admin_api

    app.register_blueprint(api, url_prefix='/etl/api')
    app.register_blueprint(admin_api, url_prefix='/etl/admin/api')
