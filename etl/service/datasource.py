from ..dao.datasource import DatasourceDao
from ..models import session_scope
from ..models.datasource import ExtDatasource


class DatasourceService(object):

    def __init__(self):
        self.__datasourceDao = DatasourceDao()

    @session_scope
    def add_datasource(self, datasource_json):
        """
        :param datasourceJson: Datasource的json格式
        :return: True OR False
        """
        try:
            self.__datasourceDao.add_datasource(datasource_json)
            return True
        except Exception as e:
            print('Error', e)
            return False

    def find_datasource_by_id(self, datasource_id):
        """

        :param id: datasource_id
        :return: 如果有数据则返回,没有返回None
        """
        try:
            datasource = self.__datasourceDao.get_model_by_id(datasource_id)
            return datasource.to_dict()
        except Exception as e:
            print('datasourceService error', e)
            return None

    def find_all(self):
        return self.__datasourceDao.find_all()

    def find_by_page_limit(self, page, per_page):
        pagination = ExtDatasource.query.paginate(page, per_page=per_page, error_out=False)
        datasource_list = pagination.items
        total = pagination.total
        return dict(items=[datasource.to_dict() for datasource in datasource_list], total=total)

    @session_scope
    def update_by_id(self, datasource_id, new_datasource_json):
        try:
            old_datasource = self.__datasourceDao.get_model_by_id(datasource_id)
            self.__datasourceDao.update(old_datasource, new_datasource_json)
            return True
        except Exception as e:
            print('datasouceService error', e)
            return False
