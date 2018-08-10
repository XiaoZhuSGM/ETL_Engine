from flask import request

from etl.constant import PER_PAGE
from etl.models import session_scope
from etl.models.ext_table_info import ExtTableInfo


class ExtTableInfoNotExist(Exception):
    def __str__(self):
        return "ext_table_info not found"


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
            query = query.filter_by(table_name=table_name)
        if record_num:
            if int(record_num) == 0:
                query = query.filter(ExtTableInfo.record_num == 0)
            else:
                query = query.filter(ExtTableInfo.record_num > 0)
        if per_page != -1:
            pagination = query.paginate(page=page, per_page=per_page, error_out=False)
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
