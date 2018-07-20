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
    mysql_db_username = ''
    mysql_db_password = ''
    mysql_db_name = ''
    mysql_db_hostname = ''
    SQLALCHEMY_ECHO = False

    SQLALCHEMY_DATABASE_URI = "mysql://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}?charset=utf8mb4".format(
        DB_USER=mysql_db_username,
        DB_PASS=mysql_db_password,
        DB_ADDR=mysql_db_hostname,
        DB_NAME=mysql_db_name)


class DevelopmentConfig(Config):
    DEBUG = True
    SQLALCHEMY_ECHO = True
    mysql_db_username = ' '
    mysql_db_password = ' '
    mysql_db_name = ' '
    mysql_db_hostname = ''

    # MySQL
    SQLALCHEMY_DATABASE_URI = "mysql://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}?charset=utf8mb4".format(
        DB_USER=mysql_db_username,
        DB_PASS=mysql_db_password,
        DB_ADDR=mysql_db_hostname,
        DB_NAME=mysql_db_name)

    SENTRY_DSN = ""
    CELERY_RESULT_BACKEND = ""
    CELERY_BROKER_URL = ""


class LocalConfig(Config):
    DEBUG = True
    mysql_db_username = 'root'
    mysql_db_password = '123456'
    mysql_db_name = 'etl'
    mysql_db_hostname = '127.0.0.1'

    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}?charset=utf8mb4".format(
        DB_USER=mysql_db_username,
        DB_PASS=mysql_db_password,
        DB_ADDR=mysql_db_hostname,
        DB_NAME=mysql_db_name)


class TestingConfig(Config):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://beanan:root@123.206.60.59:5432/test'


config = {
    'dev': DevelopmentConfig,
    'testing': TestingConfig,
    'prod': ProductionConfig,
    'default': LocalConfig,
    'local': LocalConfig
}
