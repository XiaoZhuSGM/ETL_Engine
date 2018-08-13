# -*- coding: utf-8 -*-
# @Time    : 2018/8/13 上午11:41
# @Author  : 范佳楠
from .dao import Dao
from ..models.ext_datasource_con import ExtDatasourceCon


class DatasourceConfigDao(Dao):

    def __init__(self):
        super().__init__(ExtDatasourceCon)

    def add_datasource_config(self, datasource_config):
        datasource_config = ExtDatasourceCon(**datasource_config)
        datasource_config.save()

    def find_datasource_config_by_source_id(self, source_id):
        return self.model.query.filter_by(source_id=source_id).one_or_none()

    def update(self, old_datasource_config, new_datasource_config_json):
        old_datasource_config.update(**new_datasource_config_json)
