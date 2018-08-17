from flask import request
from etl.constant import PER_PAGE
from etl.models.etl_table import ExtCleanInfo
from etl.models.etl_table import ExtTargetInfo
from etl.models.ext_table_info import ExtTableInfo
from etl.models.datasource import ExtDatasource
from etl.models import session_scope


class ExtCleanInfoParameterError(Exception):
    def __str__(self):
        return "parameter error"


class ExtCleanInfoNotFound(Exception):
    def __str__(self):
        return "ext_clean_info not found"


class ExtCleanInfoTableNotFound(Exception):
    def __init__(self, table):
        self.table = table

    def __str__(self):
        return f"{self.table}is not exist in ext_table_info"


class ExtCleanInfoColumnNotFound(Exception):
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __str__(self):
        return f"{self.table} do not have the {self.column}"


class ExtTableInfoNotFound(Exception):
    def __str__(self):
        return "ext_table_info is not found by source_id"


class ErpNotMatch(Exception):
    def __str__(self):
        return "datasrouce's erp_vendor is not match"


class ExtDatasourceNotExist(Exception):
    def __str__(self):
        return "not found datasource by source_id"


class TableNotExist(Exception):
    def __str__(self):
        return "not found table by source_id"


class ExtCleanInfoService:
    @session_scope
    def get_ext_clean_infos(self):
        source_id = request.args.get("source_id")
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", PER_PAGE))
        if page < 0 or per_page < 0:
            raise ExtCleanInfoParameterError

        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        if not datasource:
            raise ExtDatasourceNotExist

        pagination = ExtCleanInfo.query.filter_by(source_id=source_id).order_by(ExtCleanInfo.origin_table).\
                                            paginate(page, per_page=per_page, error_out=False)

        items = pagination.items
        total = pagination.total

        if total != 0:
            return total, [item.to_dict() for item in items]

        items = ExtTargetInfo.query.all()
        total = len(items)
        if total == 0:
            return total, []

        for item in items:
            ExtCleanInfo.create(source_id=source_id, target_table=item.target_table)

        pagination = ExtCleanInfo.query.filter_by(source_id=source_id).order_by(ExtCleanInfo.origin_table). \
                                        paginate(page, per_page=per_page, error_out=False)
        items = pagination.items
        total = pagination.total
        return total, [item.to_dict() for item in items]

    @session_scope
    def create_ext_clean_info(self, data):
        source_id = data.get("source_id")
        data_list = data.get("data")
        origin_table = {}
        covert_str = {}

        ext_table_infos = ExtTableInfo.query.filter_by(source_id=source_id, weight=1).all()
        if not ext_table_infos:
            raise ExtTableInfoNotFound

        ext_table_column_dict = {}
        for ext_table in ext_table_infos:
            if ext_table.alias_table_name:
                ext_table_column_dict[ext_table.alias_table_name] = ext_table.ext_column
            else:
                ext_table_column_dict[ext_table.table_name.split(".")[-1]] = ext_table.ext_column

        for data in data_list:
            table_name = data.get("origin_table")
            column_list = data.get("columns")
            covert_columns = data.get("convert_str")

            ext_column_dict = ext_table_column_dict.get(table_name)
            if not ext_column_dict:
                raise ExtCleanInfoTableNotFound(table_name)

            ext_column_dict.pop("autoincrement")
            for column in column_list:
                res = ext_column_dict.get(column)
                if not res:
                    raise ExtCleanInfoColumnNotFound(table_name, column)

            origin_table[table_name] = column_list
            if not covert_columns:
                continue
            for covert in covert_columns:
                if covert not in column_list:
                    raise ExtCleanInfoColumnNotFound(table_name, covert)
            covert_str[table_name] = covert_columns

        ExtCleanInfo.create(origin_table=origin_table, covert_str=covert_str)

    @session_scope
    def modifly_ext_clean_info(self, id, data):
        """
        [{"origin_table":"table1", "columns":["columns1", "columns2"], "convert_str":{"column1":"type1"}},
        {"origin_table":"table2", "columns":["columns1", "columns2"], "convert_str":{"column2":"type2"}},  ]
        :param id:
        :param data:
        :save: origin_table:{"t_sl_master": ['fbrh_no', 'fflow_no', 'ftrade_date', 'fcr_time', 'fsell_way'],
                            "t_sl_detail": ['fprice', 'fpack_qty', 'famt', 'fflow_no', 'fitem_subno', 'fitem_id']}

                covert_str: {"t_sl_master": {"fbrh_no": str}, "t_br_master": {"fbrh_no": str},......}

        """
        data_list = data.get("data")
        origin_table = {}
        covert_str = {}

        ext_clean_info = ExtCleanInfo.query.get(id)
        if not ext_clean_info:
            raise ExtCleanInfoNotFound

        source_id = ext_clean_info.source_id
        ext_table_infos = ExtTableInfo.query.filter_by(source_id=source_id, weight=1).all()
        if not ext_table_infos:
            raise ExtTableInfoNotFound

        ext_table_column_dict = {}
        for ext_table in ext_table_infos:
            if ext_table.alias_table_name:
                ext_table_column_dict[ext_table.alias_table_name] = ext_table.ext_column
            else:
                ext_table_column_dict[ext_table.table_name.split(".")[-1]] = ext_table.ext_column

        for data in data_list:
            table_name = data.get("origin_table")
            column_list = data.get("columns")
            covert_columns = data.get("convert_str")

            ext_column_dict = ext_table_column_dict.get(table_name)
            if not ext_column_dict:
                raise ExtCleanInfoTableNotFound(table_name)

            ext_column_dict.pop("autoincrement")
            for column in column_list:
                res = ext_column_dict.get(column)
                if not res:
                    raise ExtCleanInfoColumnNotFound(table_name, column)

            origin_table[table_name] = column_list
            if not covert_columns:
                continue
            for covert in covert_columns:
                if covert not in column_list:
                    raise ExtCleanInfoColumnNotFound(table_name, covert)
            covert_str[table_name] = covert_columns

        ext_clean_info.update(origin_table=origin_table, covert_str=covert_str)


        # for table, column_list in origin_table.items():
        #     ext_column_dict = ext_table_column_dict.get(table)
        #     if not ext_column_dict:
        #         raise ExtCleanInfoTableNotFound(table)
        #     for column in column_list:
        #         res = ext_column_dict.get(column)
        #         if not res:
        #             raise ExtCleanInfoColumnNotFound(table, column)
        #
        # for table, column_dict in covert_str.items():
        #     ext_column_dict = ext_table_column_dict.get(table)
        #     if not ext_column_dict:
        #         raise ExtCleanInfoTableNotFound(table)
        #     for column in column_dict.keys():
        #         res = ext_column_dict.get(column)
        #         if not res:
        #             raise ExtCleanInfoColumnNotFound(table, column)
        # ext_clean_info.update(origin_table=origin_table, covert_str=covert_str)

    def  get_ext_clean_info_table(self, source_id):
        tables = []

        ext_table_infos = ExtTableInfo.query.filter_by(source_id=source_id, weight=1).all()
        if not ext_table_infos:
            raise ExtTableInfoNotFound
        for ext_table in ext_table_infos:
            table_name = ext_table.alias_table_name if ext_table.alias_table_name \
                                                else ext_table.table_name.split(".")[-1]
            if table_name not in tables:
                tables.append(table_name)

        return tables

    @session_scope
    def copy_ext_clean_info(self, data):
        """
            实现同个erp下数据表配置一键复制功能
        """
        template_source_id = data.get("template_source_id")
        target_source_id = data.get("target_source_id")

        template_datasource = ExtDatasource.query.filter_by(source_id=template_source_id).first()
        target_datasource = ExtDatasource.query.filter_by(source_id=target_source_id).first()
        if not all([template_datasource, target_datasource]):
            raise ExtDatasourceNotExist

        if template_datasource.erp_vendor != target_datasource.erp_vendor:
            raise ErpNotMatch

        template_table_infos = ExtCleanInfo.query.filter_by(source_id=template_source_id).all()
        target_table_infos = ExtCleanInfo.query.filter_by(source_id=target_source_id).all()
        if not all([template_table_infos, target_table_infos]):
            raise TableNotExist

        # 同步同样表名的配置
        for template_table in template_table_infos:

            target_table = ExtCleanInfo.query.filter_by(
                source_id=target_source_id,
                target_table=template_table.target_table).first()
            if not target_table:
                continue
            info = {
                "origin_table": template_table.origin_table,
                "covert_str": template_table.covert_str,
            }
            target_table.update(**info)

