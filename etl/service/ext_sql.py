from datetime import datetime

from common.common import PAGE_SQL
from etl import db
from ..dao.datasource import DatasourceDao
from ..models import ExtDatasource, ExtTableInfo


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

    def genereate_for_full_sql(self, source_id, extract_date):
        """
        生成sql,全量的首次抓取sql
        where条件中日期字段需要处理
        :param source_id:
        :param extract_date:
        :return:
        """
        tables = db.session.query(ExtTableInfo.table_name, ExtTableInfo.filter, ExtTableInfo.filter_format,
                                  ExtTableInfo.limit_num, ExtTableInfo.order_column).filter(
            ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1).all()

        extraxt_sqls = dict(type="full", date=extract_date)
        for table in tables:
            if table.limit_num is None or table.limit_num <= 1:
                sql_str = self.common_mould(table, extract_date)
            else:
                db_type = table.datasource.db_type
                sql_str = self.page_by_limit_mould(table, db_type, extract_date)

            extraxt_sqls[table.table_name] = sql_str

        return extraxt_sqls

    def page_by_limit_mould(self, table, db_type, extract_date):
        """
        配置分页
        分页页数可以通过定时任务更新
        :param table:
        :param db_type:
        :param extract_date:
        :return:
        """
        sql_template = PAGE_SQL[db_type]

        where = ""
        if table.filter is not None:
            format_date = datetime.strptime(extract_date, table.filter_format).strftime(
                table.filter_format)
            where = table.filter.format(recorddate=format_date)
        sql_str = []
        for i in range(table.limit_num):
            sql_str.append(sql_template.format(table=table.table_name, order_rows=table.order_column, wheres=where,
                                               small=i * 200000, large=(i + 1) * 200000))
        return sql_str

    def common_mould(self, table, extract_date):
        sql_str = "select {tablename}.* from {tablename} ".format(tablename=table.table_name)
        if table.filter is not None:
            format_date = datetime.strptime(extract_date, table.filter_format).strftime(
                table.filter_format)

            sql_str = sql_str + table.filter.format(recorddate=format_date)

        return [sql_str]

    def generate_sync_sql_by_source_id(self, source_id, extract_date):
        pass
