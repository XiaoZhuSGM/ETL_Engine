"""
the app exam config file
if another app called other ,so the config file named other_config.py. and so on

"""
from typing import Optional
from kombu import Queue
import os


class Config(object):
    DEBUG = False
    TESTING = False
    PORT = 5000
    HOST = "0.0.0.0"
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_POOL_SIZE = 2
    SQLALCHEMY_MAX_OVERFLOW = 5
    # SQLALCHEMY_POOL_RECYCLE = 30 * 60
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = "B18F697BCF51AD270703BF7602C457DA"
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD: Optional[str] = None
    REDSHIFT_URL = ""

    CELERY_QUEUES = (
        Queue("etl", routing_key="etl.#"),
        Queue("rollback", routing_key="rollback.#"),
        Queue("ext_history", routing_key="ext_history.#"),
    )

    CELERY_ROUTES = {
        "etl.*": {"queue": "etl", "routing_key": "etl.#"},
        "rollback.*": {"queue": "rollback", "routing_key": "rollback.#"},
        "ext_history.*": {"queue": "ext_history", "routing_key": "ext_history.#"},
    }

    # BROKER_POOL_LIMIT = None
    BROKER_HEARTBEAT = 0


class ProductionConfig(Config):
***REMOVED***
***REMOVED***"
***REMOVED***
***REMOVED***
    SQLALCHEMY_ECHO = False

    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}".format(
        DB_USER=pgsql_db_username,
        DB_PASS=pgsql_db_password,
        DB_ADDR=pgsql_db_hostname,
        DB_NAME=pgsql_db_name,
    )

    SENTRY_DSN = ""

    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
***REMOVED***

    CELERYD_CONCURRENCY = 6
    CELERYD_MAX_TASKS_PER_CHILD = 100
***REMOVED***
***REMOVED***

    AIRFLOW_DB_URL = (
***REMOVED***
    )
    REDSHIFT_URL = os.environ.get('REDSHIFT_URL')

    CACHE_TYPE = "redis"
    CACHE_REDIS_HOST = REDIS_HOST
    CACHE_REDIS_PORT = REDIS_PORT
    CACHE_REDIS_PASSWORD = REDIS_PASSWORD
    CACHE_REDIS_DB = REDIS_DB


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_ECHO = True

***REMOVED***
***REMOVED***
    postgresql_db_name = "cm_etl"
***REMOVED***
    # postgresql
    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}".format(
        DB_USER=postgresql_db_username,
        DB_PASS=postgresql_db_password,
        DB_ADDR=postgresql_db_hostname,
        DB_NAME=postgresql_db_name,
    )

    SENTRY_DSN = "http://0ed8df75ac66462bb8a82064955052ad@sentry-dev.chaomengdata.com/9"

    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None

    CELERYD_CONCURRENCY = 6
    CELERYD_MAX_TASKS_PER_CHILD = 100
    # CELERY_RESULT_BACKEND = (
    #     f"redis://:{REDIS_PASSWORD}@redis:{REDIS_PORT}/11"
    #     if REDIS_PASSWORD
    #     else f"redis://redis:{REDIS_PORT}/11"
    # )
    # CELERY_BROKER_URL = (
    #     f"redis://:{REDIS_PASSWORD}@redis:{REDIS_PORT}/10"
    #     if REDIS_PASSWORD
    #     else f"redis://redis:{REDIS_PORT}/10"
    # )

***REMOVED***
***REMOVED***

    AIRFLOW_DB_URL = (
***REMOVED***
    )
    REDSHIFT_URL = os.environ.get('REDSHIFT_URL')

    CACHE_TYPE = "null"  # debug, disable cache
    CACHE_REDIS_HOST = REDIS_HOST
    CACHE_REDIS_PORT = REDIS_PORT
    CACHE_REDIS_PASSWORD = REDIS_PASSWORD
    CACHE_REDIS_DB = REDIS_DB


class LocalConfig(Config):
    DEBUG = True
    pgsql_db_username = "root"
    pgsql_db_password = "123456"
    pgsql_db_name = "etl"
    pgsql_db_hostname = "127.0.0.1"

    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}".format(
        DB_USER=pgsql_db_username,
        DB_PASS=pgsql_db_password,
        DB_ADDR=pgsql_db_hostname,
        DB_NAME=pgsql_db_name,
    )

    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None

    CELERYD_CONCURRENCY = 6
    CELERYD_MAX_TASKS_PER_CHILD = 100
    CELERY_RESULT_BACKEND = (
        f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/11"
        if REDIS_PASSWORD
        else f"redis://{REDIS_HOST}:{REDIS_PORT}/11"
    )
    CELERY_BROKER_URL = (
        f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/10"
        if REDIS_PASSWORD
        else f"redis://{REDIS_HOST}:{REDIS_PORT}/10"
    )

    AIRFLOW_DB_URL = (
***REMOVED***
    )
    REDSHIFT_URL = os.environ.get('REDSHIFT_URL')


class TestingConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = (
        "postgresql+psycopg2://beanan:root@123.206.60.59:5432/test"
    )
    SENTRY_DSN = ""


class UnitestConfig(Config):
    TESTING = True
    SENTRY_DSN = ""
***REMOVED***
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None
    CELERY_BROKER_URL = "pyamqp://guest@123.206.60.59//"
    CELERY_RESULT_BACKEND = "rpc://guest@123.206.60.59//"


class DockerDevConfig(DevelopmentConfig):
    REDIS_HOST = "redis"


class DockerProdConfig(ProductionConfig):
    REDIS_HOST = "redis"


config = {
    "dev": DevelopmentConfig,
    "testing": TestingConfig,
    "prod": ProductionConfig,
    "default": LocalConfig,
    "local": LocalConfig,
    "unittest": UnitestConfig,
    "docker_dev": DockerDevConfig,
    "docker_prod": DockerProdConfig,
}
