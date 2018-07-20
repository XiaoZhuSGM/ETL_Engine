
from ..models.datasource import Datasource
from ..dao.datasource import DatasourceDao

class DatasourceService(object):

    def __init__(self):
        self.__datasourceDao = DatasourceDao()

    def add_datasource(self, datasourceJson):
        """
        :param datasourceJson: Datasource的json格式
        :return: True OR False
        """
        try:
            self.__datasourceDao.add_datasource(datasourceJson)
            return True
        except Exception as e:
            print('Error', e)
            return False

    def find_datasource_by_id(self, id):
        """

        :param id: datasourceId
        :return: 如果有数据则返回,没有返回None
        """
        try:
            datasource = self.__datasourceDao.find_by_id(id)
            return Datasource.datasourceToDict(datasource)
        except Exception as e:
            print('datasourceService error', e)
            return None

    def find_all(self):
        return self.__datasourceDao.find_all()

    def find_by_page_limit(self, page, limit):
        pagination = Datasource.query.paginate(page, per_page=limit, error_out=False)
        datasource_list = pagination.items
        total = pagination.total
        return dict(items=[datasource.datasourceToDict(datasource) for datasource in datasource_list], total=total)

    def update_by_id(self, id, new_datasource_json):

        try:
            old_datasource = self.__datasourceDao.find_by_id(id)
            self.__datasourceDao.update(old_datasource, new_datasource_json)
            return True
        except Exception as e:
            print('datasouceService error', e)
            return False
