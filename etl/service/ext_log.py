from etl import db
from ..models import ExtLogInfo, session_scope
from sqlalchemy import desc
from etl.constant import PER_PAGE


class ExtLogSqlService(object):

    def __init__(self):
        pass

    @session_scope
    def add_log(self, **kwargs):
        """
        添加日志
        :param kwargs:
        :return:
        """
        log = ExtLogInfo.create(**kwargs)
        return log

    def get_log(self, match_term):
        result = dict()
        query = db.session.query(ExtLogInfo)
        if "source_id" in match_term:
            query = query.filter(ExtLogInfo.source_id == match_term["source_id"])

        if "table_name" in match_term:
            query = query.filter(ExtLogInfo.table_name == match_term["table_name"])

        if "task_type" in match_term:
            query = query.filter(ExtLogInfo.task_type == match_term["task_type"])

        if "start_time" in match_term:
            query = query.filter(ExtLogInfo.created_at >= match_term["start_time"])

        if "end_time" in match_term:
            query = query.filter(ExtLogInfo.created_at <= match_term["end_time"])

        if "result" in match_term:
            query = query.filter(ExtLogInfo.result == match_term["result"])

        query.order_by(desc(ExtLogInfo.created_at))

        pagination = query.paginate(page=int(match_term["page"]),
                                    per_page=int(match_term["per_page"]) if "per_page" in match_term else PER_PAGE)
        result["total"], result["page"] = pagination.total, pagination.page
        result["items"] = [item.to_dict() for item in pagination.items]
        return result
