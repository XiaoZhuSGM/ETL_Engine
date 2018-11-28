import json
from collections import defaultdict
from datetime import datetime, timedelta
from sqlalchemy.orm import joinedload
from sqlalchemy import or_

from common.common import (
    PAGE_SQL,
    upload_body_to_s3,
    SQL_PREFIX,
    now_timestamp,
    S3_BUCKET,
)
from etl import db
from ..models import ExtDatasource, ExtTableInfo, ExtCleanInfo
from etl.service.ext_clean_info import ExtCleanInfoService

clean_info_service = ExtCleanInfoService()


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

    def generate_rollback_sql(self, source_id, extract_date, targets):
        original_table = []
        for target in targets:
            data = clean_info_service.get_ext_clean_info_target(source_id, target)
            temp_tables = data["origin_table"].keys()
            original_table.extend(temp_tables)

        original_table = [table.lower() for table in original_table]
        
        return self.generate_table_sql(source_id, original_table, extract_date)

    def generate_full_sql(self, source_id, extract_date):
        """
        生成sql,全量的首次抓取sql
        where条件中日期字段需要处理
        :param source_id:
        :param extract_date:
        :return:
        """
        tables = (
            db.session.query(ExtTableInfo)
            .filter(ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1)
            .options(joinedload(ExtTableInfo.datasource))
            .all()
        )

        tables_sqls = {
            "type": "full",
            "query_date": extract_date,
            "source_id": source_id,
            "sqls": self._generate_by_correct_mould(tables, extract_date),
        }

        file_name = str(now_timestamp()) + ".json"
        key = SQL_PREFIX.format(source_id=source_id, date=extract_date) + file_name
        upload_body_to_s3(S3_BUCKET, key, json.dumps(tables_sqls))
        return file_name

    def generate_table_sql(self, source_id, targets, extract_date):
        tables = (
            db.session.query(ExtTableInfo)
            .filter(ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1)
            .options(joinedload(ExtTableInfo.datasource))
            .all()
        )
        tables_sqls = {
            "type": "tables",
            "query_date": extract_date,
            "source_id": source_id,
        }

        total_sqls = {
            table_name.split(".")[-1]: sqls
            for table_name, sqls in self._generate_by_correct_mould(
                tables, extract_date
            ).items()
        }

        target_sqls = {target: total_sqls[target] for target in targets}

        tables_sqls["sqls"] = target_sqls

        file_name = str(now_timestamp()) + ".json"
        key = SQL_PREFIX.format(source_id=source_id, date=extract_date) + file_name
        upload_body_to_s3(S3_BUCKET, key, json.dumps(tables_sqls))
        return file_name

    def _generate_by_correct_mould(self, tables, extract_date):
        sqls = defaultdict(list)
        for table in tables:
            if table.limit_num is None or table.limit_num <= 1:
                sql_str = self._common_mould(table, extract_date)
            else:
                db_type = table.datasource.db_type
                sql_str = self._page_by_limit_mould(table, db_type, extract_date)

            table_name = (
                table.alias_table_name
                if table.alias_table_name
                else table.table_name.split(".")[-1]
            )
            table_name = table_name.lower()
            sqls[table_name].extend(sql_str)
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
            format_date = datetime.strptime(extract_date, "%Y-%m-%d")
            where = self._formated_where(table, format_date)
        sql_str = []
        for i in range(table.limit_num):
            order_by = table.order_column
            if not order_by:
                order_by = table.ext_pri_key
            if not order_by:
                order_by = ""
            if order_by:
                order_by = f"order by {order_by} desc"

            sql_str.append(
                sql_template.format(
                    table=table.table_name,
                    order_by=order_by,
                    wheres=where,
                    small=i * 200_000,
                    large=(i + 1) * 200_000,
                )
            )
        return sql_str

    def _common_mould(self, table, extract_date):
        if table.special_column:
            sql_str = "select {special_column} from {tablename} ".format(
                tablename=table.table_name, special_column=table.special_column
            )
        else:
            sql_str = "select {tablename}.* from {tablename} ".format(
                tablename=table.table_name
            )
        if table.filter is not None:
            format_date = datetime.strptime(extract_date, "%Y-%m-%d")
            sql_str = sql_str + self._formated_where(table, format_date)

        return [sql_str]

    def generate_sync_sql_by_source_id(self, source_id, extract_date):
        pass

    def _formated_where(self, table, date):
        if not table.filter_format:
            return table.filter
        recorddate = date.strftime(table.filter_format)
        date_s = date.strftime(table.filter_format)
        date_e = (date + timedelta(days=1)).strftime(table.filter_format)
        return table.filter.format(recorddate=recorddate, date_s=date_s, date_e=date_e)

    def display_full_sql(self, source_id, extract_date):
        """
        查看根据抓表策略生成的full sql，不存入到s3
        :param source_id:
        :param extract_date:
        :return:
        """
        tables = (
            db.session.query(ExtTableInfo)
            .filter(ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1)
            .options(joinedload(ExtTableInfo.datasource))
            .all()
        )

        return self._generate_by_correct_mould(tables, extract_date)

    def generate_target_sql(self, source_id, extract_date, target_table):
        """
        根据目标表生产sql,用于只抓取特定的目标表的数据
        """
        ext_clean_info_models = (
            db.session.query(ExtCleanInfo)
            .filter(
                ExtCleanInfo.source_id == source_id,
                ExtCleanInfo.deleted == False,
                ExtCleanInfo.target_table.in_(target_table),
            )
            .all()
        )
        origin_table = set()
        for model in ext_clean_info_models:
            if model.origin_table:
                origin_table.update(model.origin_table.keys())
        total_table = []
        for table_name in origin_table:
            tables = (
                db.session.query(ExtTableInfo)
                .filter(ExtTableInfo.source_id == source_id, ExtTableInfo.weight == 1)
                .filter(
                    or_(
                        ExtTableInfo.table_name.ilike(f"%.{table_name}"),
                        ExtTableInfo.table_name.ilike(table_name),
                        ExtTableInfo.alias_table_name == table_name,
                    )
                )
                .options(joinedload(ExtTableInfo.datasource))
                .all()
            )
            total_table.extend(tables)

        tables_sqls = {
            "type": "tables",
            "query_date": extract_date,
            "source_id": source_id,
            "sqls": self._generate_by_correct_mould(total_table, extract_date),
        }
        file_name = str(now_timestamp()) + ".json"
        key = SQL_PREFIX.format(source_id=source_id, date=extract_date) + file_name
        upload_body_to_s3(S3_BUCKET, key, json.dumps(tables_sqls))
        return file_name
