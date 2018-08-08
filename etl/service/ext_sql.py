from ..dao.datasource import DatasourceDao
from ..models import session_scope
from ..models import ExtDatasource, ExtTableInfo
from datetime import datetime

from etl import db


class DatasourceSqlService(object):

    def __init__(self):
        self.__datasourceDao = DatasourceDao()

    def sync_sql_by_source_id(self, source_id, extract_date):
        data_source = ExtDatasource.query.filter_by(source_id=source_id).one_or_none()
        if data_source.erp_vendor == "科脉云鼎":
            self.generate_sync_sql_by_source_id(source_id, extract_date)
            pass
        elif data_source.erp_vendor == "海鼎":
            pass
        else:
            pass

    @staticmethod
    def genereate_for_full_sql(source_id, extract_date):
        """
        生成sql,全量的首次抓取sql
        where条件中日期字段需要处理
        :param source_id:
        :param extract_date:
        :return:
        """
        tables = db.session.query(ExtTableInfo.table_name, ExtTableInfo.filter, ExtTableInfo.filter_format).filter(
            ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1).all()

        extraxt_sqls = dict(type="full", date=extract_date)
        for table in tables:
            sql_str = "select {tablename}.* from {tablename} ".format(tablename=table.table_name)
            if table.filter is not None:
                format_date = datetime.strptime(extract_date, table.filter_format).strftime(
                    table.filter_format)

                sql_str = sql_str + table.filter.format(recorddate=format_date)
            extraxt_sqls[table.table_name] = [sql_str]

        return extraxt_sqls

    def generate_sync_sql_by_source_id(self, source_id, extract_date):
        pass
