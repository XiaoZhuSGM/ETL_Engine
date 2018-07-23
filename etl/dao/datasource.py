import json

from ..etl import db
from ..models.datasource import ExtDatasource


class DatasourceDao():

    def find_by_id(self, id):
        datasource = db.session.query(ExtDatasource).filter_by(id=id).one()
        return datasource

    def add_datasource(self, datasource_json):
        datasource = ExtDatasource.fromJson(datasource_json)
        datasource.save()
        db.session.commit()

    def find_all(self):
        datasource_list = db.session.query(ExtDatasource).all()
        return datasource_list

    def update(self, old_datasource, new_datasource_json):
        old_datasource.update(**new_datasource_json)
        db.session.commit()
