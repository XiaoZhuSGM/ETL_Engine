from flask import request
from etl.constant import PER_PAGE
from etl.models import session_scope
from etl.models.ext_table_info import ExtTableInfo
from etl.models.datasource import ExtDatasource


class ExtTableInfoNotExist(Exception):
    def __str__(self):
        return "ext_table_info not found"


class ErpNotMatch(Exception):
    def __str__(self):
        return "datasrouce's erp_vendor is not match"


class ExtDatasourceNotExist(Exception):
    def __str__(self):
        return "not found datasource by source_id"


class TableNotExist(Exception):
    def __str__(self):
        return "not found table by source_id"


class ExtTableInfoService:
    def default_dictify(self, eti):
        return {
            "id": eti.id,
            "table_name": eti.table_name,
            "ext_column": eti.ext_column,
            "ext_pri_key": eti.ext_pri_key,
            "record_num": eti.record_num,
            "order_column": eti.order_column.split(",") if eti.order_column else [],
            "sync_column": eti.sync_column.split(",") if eti.order_column else [],
            "limit_num": eti.limit_num,
            "filter": eti.filter,
            "filter_format": eti.filter_format,
            "weight": eti.weight,
            "alias_table_name": eti.alias_table_name,
            "created_at": eti.created_at,
            "updated_at": eti.updated_at,
        }

    def get_ext_table_infos(self, args):
        """获取对应条件的 ext_table_infos.

        :param args: args
        :type args: dict
        :return: 总数, ext_table_info 详情的列表.
        :rtype: tuple
        """
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", PER_PAGE))
        source_id = args.get("source_id")
        weight = args.get("weight")
        table_name = args.get("table_name")
        record_num = args.get("record_num")
        query = ExtTableInfo.query
        if source_id:
            query = query.filter_by(source_id=source_id)
        if weight:
            query = query.filter_by(weight=int(weight))
        if table_name:
            query = query.filter(ExtTableInfo.table_name.contains(table_name, autoescape=True))
        if record_num:
            record_num = int(record_num) if record_num.isdigit() else 0
            query = query.filter(ExtTableInfo.record_num >= record_num)
        if per_page != -1:
            pagination = query.order_by(ExtTableInfo.table_name).paginate(page=page, per_page=per_page, error_out=False)
            items = pagination.items
            total = pagination.total
        else:
            items = query.all()
            total = len(items)
        return total, [self.default_dictify(eti) for eti in items]

    @session_scope
    def create_ext_table_info(self, info):
        """创建 ext_table_info.
        :param info: info 值.
        :type info: dict
        :return: ExtTableInfo
        :rtype: ExtTableInfo
        """

        ext_table_info = ExtTableInfo(**info)
        ext_table_info.save()
        return ext_table_info

    def get_ext_table_info(self, id):
        """获取单个 ext_table_info.

        :param id: ExtTableInfo.id
        :type id: int
        :raises ExtTableInfoNotExist: ExtTableInfo.id 不存在
        :return: ExtTableInfo 的详情
        :rtype: dict
        """

        ext_table_info = ExtTableInfo.query.get(id)
        if not ext_table_info:
            raise ExtTableInfoNotExist()
        return self.default_dictify(ext_table_info)

    @session_scope
    def modify_ext_table_info(self, id, info):
        """修改单个 ext_table_info.

        :param id: ExtTableInfo.id
        :type id: int
        :param info: 要修改的内容
        :type info: dict
        :raises ExtTableInfoNotExist: ExtTableInfo.id 不存在
        :return: ExtTableInfo
        :rtype: ExtTableInfo
        """

        ext_table_info = ExtTableInfo.query.get(id)
        if not ext_table_info:
            raise ExtTableInfoNotExist()
        ext_table_info.update(**info)
        return ext_table_info

    @session_scope
    def copy_ext_table_info(self, data):
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
        template_table_infos = ExtTableInfo.query.filter_by(source_id=template_source_id).all()
        target_table_infos = ExtTableInfo.query.filter_by(source_id=target_source_id).all()
        if not all([template_table_infos, target_table_infos]):
            raise TableNotExist

        # 同步同样表名的配置
        for template_table in template_table_infos:

            target_table = ExtTableInfo.query.filter_by(
                source_id=target_source_id,
                table_name=template_table.table_name).first()
            if not target_table:
                continue
            info = {
                "alias_table_name": template_table.alias_table_name,
                "order_column": template_table.order_column,
                "sync_column": template_table.sync_column,
                "limit_num": template_table.limit_num,
                "filter": template_table.filter,
                "filter_format": template_table.filter_format,
                "weight": template_table.weight,
                "strategy": template_table.strategy
            }
            target_table.update(**info)

    @session_scope
    def batch_ext_table_info(self, data):
        """
            批量配置相似表的策略
        """
        table_id_list = data.pop("id")
        total, success_num = len(table_id_list), 0
        if data.get("sync_column") is not None:
            data["sync_column"] = ",".join(data["sync_column"])
        if data.get("order_column") is not None:
            data["order_column"] = ",".join(data["order_column"])

        for table_id in table_id_list:
            ext_table_info = ExtTableInfo.query.get(table_id)
            if not ext_table_info:

                continue
            ext_table_info.update(**data)
            success_num += 1
        return total, success_num
