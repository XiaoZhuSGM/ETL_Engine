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
        datasource_list = db.session.query(ExtDatasource).order_by(ExtDatasource.source_id.asc()).all()
        return datasource_list

    def update(self, old_datasource, new_datasource_json):
        old_datasource.update(**new_datasource_json)

    def find_datasource_by_source_id(self, source_id):
        return self.model.query.filter_by(source_id=source_id).one_or_none()

    def find_datasource_by_erp(self, erp_vendor):
        condition = f'%{erp_vendor}%'
        return self.model.query.filter(
            ExtDatasource.erp_vendor.like(condition)).all()
