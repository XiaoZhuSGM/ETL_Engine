from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from raven.contrib.flask import Sentry

__all__ = ['create_app']

db = SQLAlchemy()

DEFAULT_APP_NAME = 'etl'


def create_app(config=None):
    app = Flask(DEFAULT_APP_NAME)
    if config is not None:
        app.config.from_object(config)
    db.init_app(app)

    configure_path_converter(app)
    configure_blueprints(app)
    configure_sentry(app)

    CORS(app, resources={r"/etl/admin_api/*": {"origins": "*"}})
    CORS(app, resources={r"/etl/api/*": {"origins": "*"}})

    return app


def configure_sentry(app):
    sentry = Sentry(dsn='https://<key>:<secret>@sentry.io/<project>')
    sentry.init_app(app)


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
