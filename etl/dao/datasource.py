import json

from ..etl import db
from ..models.datasource import Datasource


class DatasourceDao():

    def find_by_id(self, id):
        datasource = db.session.query(Datasource).filter_by(id=id).one()
        return datasource

    def add_datasource(self, datasourceJson):
        datasource = Datasource.fromJson(datasourceJson)
        datasource.save()
        db.session.commit()

    def find_all(self):
        datasource_list = db.session.query(Datasource).all()
        return datasource_list

    def update(self, old_datasource, new_datasource_json):
        # datasource_dict = json.loads(new_datasource_json)
        old_datasource.update(**new_datasource_json)
        db.session.commit()
