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
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
    SENTRY_DSN = ""
    CELERYD_CONCURRENCY = 6
    CELERYD_MAX_TASKS_PER_CHILD = 100
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND")
    AIRFLOW_DB_URL = os.environ.get("AIRFLOW_DB_URL")
    REDSHIFT_URL = os.environ.get("REDSHIFT_URL")

    CACHE_TYPE = "redis"
    CACHE_REDIS_HOST = "redis"
    CACHE_REDIS_PORT = 6379
    CACHE_REDIS_DB = 1


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_ECHO = True
    SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
    SENTRY_DSN = "http://0ed8df75ac66462bb8a82064955052ad@sentry-dev.chaomengdata.com/9"
    CELERYD_CONCURRENCY = 6
    CELERYD_MAX_TASKS_PER_CHILD = 100
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND")
    AIRFLOW_DB_URL = os.environ.get("AIRFLOW_DB_URL")
    REDSHIFT_URL = os.environ.get("REDSHIFT_URL")

    CACHE_TYPE = "redis"
    CACHE_REDIS_HOST = "redis"
    CACHE_REDIS_PORT = 6379
    CACHE_REDIS_DB = 1


class LocalConfig(Config):
    pass


class TestingConfig(Config):
    pass


class UnitestConfig(Config):
    TESTING = True
    SENTRY_DSN = ""
    SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND")


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
