from datetime import datetime, timedelta

from ..dao.datasource import DatasourceDao
from ..dao.datasource_config import DatasourceConfigDao
from ..models import session_scope
from ..models.datasource import ExtDatasource


class ExtDatasourceNotExist(Exception):
    def __str__(self):
        return "ext_datasource not found"


class ExtDatasourceConfigNotExist(Exception):
    def __str__(self):
        return "ext_datasource_config not found"


class DatasourceService(object):

    def __init__(self):
        self.__datasourceDao = DatasourceDao()
        self.__datasourceConfigDao = DatasourceConfigDao()
        self.db_url_template = {
            'sqlserver': 'mssql+pymssql://{username}:{password}@{host}:{port}/{db_name}',
            'postgresql': 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}',
            'oracle': 'oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={db_name}'
        }

    @session_scope
    def add_datasource(self, datasource_add_config_json):
        """
        :param datasource_add_config_json datasource和datasource_config两个实体类的json
        :return: True OR False
        """
        datasource_json = datasource_add_config_json['datasource']
        datasource_config_json = datasource_add_config_json['datasource_config']
        self.__datasourceDao.add_datasource(datasource_json)
        self.__datasourceConfigDao.add_datasource_config(datasource_config_json)
        return True

    def find_datasource_by_source_id(self, source_id):
        """
        :param id: datasource_id
        :return: 如果有数据则返回,没有返回None
        """
        datasource = self.__datasourceDao.find_datasource_by_source_id(source_id)

        if not datasource:
            raise ExtDatasourceNotExist()

        return datasource.to_dict_and_config()

    def find_all(self):
        return self.__datasourceDao.find_all()

    def find_by_page_limit(self, page, per_page):
        pagination = ExtDatasource.query.order_by(ExtDatasource.source_id.asc()).paginate(page,
                                                                                          per_page=per_page,
                                                                                          error_out=False)
        datasource_list = pagination.items
        total = pagination.total
        return dict(items=[datasource.to_dict_and_config() for datasource in datasource_list], total=total)

    @session_scope
    def update_by_id(self, datasource_id, new_datasource_and_config_json):

        new_datasource_json = new_datasource_and_config_json['datasource']
        new_datasource_config_json = new_datasource_and_config_json['datasource_config']
        old_datasource = self.__datasourceDao.get_model_by_id(datasource_id)

        if not old_datasource:
            raise ExtDatasourceNotExist()

        old_datasource_config = old_datasource.ext_datasource_config

        if not old_datasource_config:
            self.__datasourceConfigDao.add_datasource_config(new_datasource_config_json)
        else:
            self.__datasourceConfigDao.update(old_datasource_config, new_datasource_config_json)

        self.__datasourceDao.update(old_datasource, new_datasource_json)

        return True

    def find_datasource_by_erp(self, erp_vendor):
        return self.__datasourceDao.find_datasource_by_erp(erp_vendor)

    def generator_full_crontab_expression(self, source_id):
        """
        根据source_id生成cron表达式
        :param source_id: source_id
        :return: cron表达式
        """
        datasource_config = self.__datasourceConfigDao.find_datasource_config_by_source_id(source_id)

        if not self.check_datasource_config_empty(datasource_config):
            return None
        # 0 8-23/{frequency} * * *
        # 02:02:07
        ext_time = datasource_config.ext_time
        frequency = datasource_config.frequency
        cron_list = [x.lstrip('0') for x in ext_time.split(':')]
        cron_list = [x if x else '0' for x in cron_list]
        hour = cron_list[0]
        minute = cron_list[1]
        cron_expression = f'{minute} {hour} * * *'
        return cron_expression

    def check_datasource_config_empty(self, datasource_config):
        return datasource_config and datasource_config.frequency and datasource_config.ext_time

    def generator_extract_event(self, source_id):
        """
        根据source_id来生成对应的extract_event
        event = dict(source_id="59YYYYYYYYYYYYY", query_date="2018-08-12", task_type="full",
                 # filename="2018-08-13 16:32:40.557536.json",
                 db_url="mssql+pymssql://adbcmsj:adb88537660@36.41.172.83:1800/adbdb")
        :param source_id:
        :return:
        """
        # 1. 计算日期 计算日期
        datasource = self.__datasourceDao.find_datasource_by_source_id(source_id)
        if not datasource:
            raise ExtDatasourceNotExist()

        delta = datasource.delta
        now = datetime.now()
        query_date = (now + timedelta(days=-delta)).strftime('%Y-%m-%d')

        # 2. 得到db_url
        db_url = self.generator_db_url(datasource)

        # 3. 计算task_type类型是full还是增量
        task_type = self.generator_task_type(source_id, query_date)

        event = dict(source_id=source_id, query_date=query_date, task_type=task_type, db_url=db_url)
        return event

    def generator_task_type(self, source_id, query_date):
        return 'full'

    def generator_db_url(self, datasource):
        db_type = datasource.db_type
        db_name = datasource.db_name['database']
        username = datasource.username
        password = datasource.password
        host = datasource.host
        port = datasource.port

        if not all([db_type, db_name, username, password, host, port]):
            return "DB_url parameter is missing"

        database_url = self.db_url_template[db_type].format(
            username=username,
            password=password,
            host=host,
            port=port,
            db_name=db_name)

        return database_url
