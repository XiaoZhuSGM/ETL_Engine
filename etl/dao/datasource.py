from .dao import Dao
from ..etl import db
from ..models.datasource import ExtDatasource


class DatasourceDao(Dao):

    def __init__(self):
        super().__init__(ExtDatasource)

    def add_datasource(self, datasource_json):
        datasource = ExtDatasource(**datasource_json)
        datasource.save()

    def find_all(self):
        datasource_list = db.session.query(ExtDatasource).all()
        return datasource_list

    def update(self, old_datasource, new_datasource_json):
        old_datasource.update(**new_datasource_json)
