"""
the app exam config file
if another app called other ,so the config file named other_config.py. and so on

"""


class Config(object):
    DEBUG = False
    TESTING = False
    PORT = 5000
    HOST = "0.0.0.0"
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_POOL_SIZE = 10
    SQLALCHEMY_MAX_OVERFLOW = 10
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = "score-center"


class ProductionConfig(Config):
    pgsql_db_username = ''
    pgsql_db_password = ''
    pgsql_db_name = ''
    pgsql_db_hostname = ''
    SQLALCHEMY_ECHO = False

    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}".format(
        DB_USER=pgsql_db_username,
        DB_PASS=pgsql_db_password,
        DB_ADDR=pgsql_db_hostname,
        DB_NAME=pgsql_db_name)


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_ECHO = True

***REMOVED***
***REMOVED***
    postgresql_db_name = 'cm_etl'
    postgresql_db_hostname = 'cm-std.cdl8ar96w1hm.rds.cn-north-1.amazonaws.com.cn:5432'
    # postgresql
    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}".format(
        DB_USER=postgresql_db_username,
        DB_PASS=postgresql_db_password,
        DB_ADDR=postgresql_db_hostname,
        DB_NAME=postgresql_db_name)

    SENTRY_DSN = "http://0ed8df75ac66462bb8a82064955052ad@sentry-dev.chaomengdata.com/9"
    CELERY_RESULT_BACKEND = ""
    CELERY_BROKER_URL = ""


class LocalConfig(Config):
    DEBUG = True
    pgsql_db_username = 'root'
    pgsql_db_password = '123456'
    pgsql_db_name = 'etl'
    pgsql_db_hostname = '127.0.0.1'

    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}".format(
        DB_USER=pgsql_db_username,
        DB_PASS=pgsql_db_password,
        DB_ADDR=pgsql_db_hostname,
        DB_NAME=pgsql_db_name)


class TestingConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://beanan:root@123.206.60.59:5432/test'
    SENTRY_DSN = ""


class UnitestConfig(Config):
    TESTING = True


config = {
    'dev': DevelopmentConfig,
    'testing': TestingConfig,
    'prod': ProductionConfig,
    'default': LocalConfig,
    'local': LocalConfig,
    'unittest': UnitestConfig,
}
