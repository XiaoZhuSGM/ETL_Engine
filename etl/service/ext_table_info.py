from etl.models.ext_table_info import ExtTableInfo
from etl.models import session_scope
from flask import request
from etl.constant import PER_PAGE


class ExtTableInfoNotExist(Exception):
    def __str__(self):
        return "ext_table_info not found"


class ExtTableInfoService:
    def default_dictify(self, ext_table_info):
        return {
            "id": ext_table_info.id,
            "cmid": ext_table_info.cmid,
            "table_name": ext_table_info.table_name,
            "ext_column": ext_table_info.ext_column,
            "ext_pri_key": ext_table_info.ext_pri_key,
            "record_num": ext_table_info.record_num,
            "order_column": ext_table_info.order_column.split(","),
            "sync_column": ext_table_info.sync_column.split(","),
            "limit_num": ext_table_info.limit_num,
            "filter": ext_table_info.filter,
            "filter_format": ext_table_info.filter_format,
            "weight": ext_table_info.weight,
            "created_at": ext_table_info.created_at,
            "updated_at": ext_table_info.updated_at,
        }

    def get_ext_table_infos(self, cmid):
        """获取对应 cmid 的 ext_table_infos.

        :param cmid: cmid
        :type cmid: int
        :return: 总数, ext_table_info 详情的列表.
        :rtype: tuple
        """
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", PER_PAGE))
        query = ExtTableInfo.query.filter_by(cmid=cmid)
        total = query.order_by(None).count()
        items = query.limit(per_page).offset((page - 1) * per_page)
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
