from datetime import datetime

from common.common import PAGE_SQL
from etl import db
from ..models import ExtDatasource, ExtTableInfo
from collections import defaultdict


class DatasourceSqlService(object):
    def sync_sql_by_source_id(self, source_id, extract_date):
        data_source = ExtDatasource.query.filter_by(source_id=source_id).one_or_none()
        if data_source.erp_vendor == "科脉云鼎":
            self.generate_sync_sql_by_source_id(source_id, extract_date)
            pass
        elif data_source.erp_vendor == "海鼎":
            pass
        else:
            pass

    def generate_full_sql(self, source_id, extract_date):
        """
        生成sql,全量的首次抓取sql
        where条件中日期字段需要处理
        :param source_id:
        :param extract_date:
        :return:
        """
        tables = (
            db.session.query(
                ExtTableInfo.table_name,
                ExtTableInfo.filter,
                ExtTableInfo.filter_format,
                ExtTableInfo.limit_num,
                ExtTableInfo.order_column,
                ExtTableInfo.alias_table_name
            ).filter(ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1).all()
        )

        return {
            "type": "full",
            "date": extract_date,
            "sqls": self._generate_by_correct_mould(tables, extract_date)

        }

    def generate_table_sql(self, source_id, table_names, extract_date):
        tables = (
            db.session.query(
                ExtTableInfo.table_name,
                ExtTableInfo.filter,
                ExtTableInfo.filter_format,
                ExtTableInfo.limit_num,
                ExtTableInfo.order_column,
                ExtTableInfo.alias_table_name
            )
                .filter(
                ExtTableInfo.source_id == source_id,
                ExtTableInfo.weight == 1,
                ExtTableInfo.table_name.in_(table_names.split(",")),
            ).all()
        )
        return {
            "type": "single_table",
            "date": extract_date,
            "sqls": self._generate_by_correct_mould(tables, extract_date)
        }

    def _generate_by_correct_mould(self, tables, extract_date):
        sqls = defaultdict(list)
        for table in tables:
            if table.limit_num is None or table.limit_num <= 1:
                sql_str = self._common_mould(table, extract_date)
            else:
                db_type = table.datasource.db_type
                sql_str = self._page_by_limit_mould(table, db_type, extract_date)

            sqls[table.alias_table_name if table.alias_table_name else table.table_name].extend(sql_str)
        return sqls

    def _page_by_limit_mould(self, table, db_type, extract_date):
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
            format_date = datetime.strptime(extract_date, "%Y-%m-%d").strftime(
                table.filter_format
            )
            where = table.filter.format(recorddate=format_date)
        sql_str = []
        for i in range(table.limit_num):
            sql_str.append(
                sql_template.format(
                    table=table.table_name,
                    order_rows=table.order_column,
                    wheres=where,
                    small=i * 200000,
                    large=(i + 1) * 200000,
                )
            )
        return sql_str

    def _common_mould(self, table, extract_date):
        sql_str = "select {tablename}.* from {tablename} ".format(
            tablename=table.table_name
        )
        if table.filter is not None:
            format_date = datetime.strptime(extract_date, "%Y-%m-%d").strftime(
                table.filter_format
            )

            sql_str = sql_str + table.filter.format(recorddate=format_date)

        return [sql_str]

    def generate_sync_sql_by_source_id(self, source_id, extract_date):
        pass
