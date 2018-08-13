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
