from etl import db
from ..models import ExtLogInfo, session_scope
from sqlalchemy import desc
from etl.constant import PER_PAGE
from sqlalchemy import or_


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

    def get_success_rate(self, match_term):
        result = dict()
        query = db.session.query(ExtLogInfo)
        if "source_id" in match_term:
            query = query.filter(ExtLogInfo.source_id == match_term["source_id"])

        if "table_name" in match_term:
            query = query.filter(ExtLogInfo.table_name == match_term["table_name"])

        if "task_type" in match_term:
            query = query.filter(ExtLogInfo.task_type == match_term["task_type"])

        if "start_time" in match_term:
            query = query.filter(ExtLogInfo.extract_date >= match_term["start_time"])

        if "end_time" in match_term:
            query = query.filter(ExtLogInfo.extract_date <= match_term["end_time"])

        """
            计算成功率
            总成功率
                总成功数 / 总日志条数
            1. 抓数
                抓数的总成功率
                    S(1) + S(12) / T(1) + T(12)
                首次抓数的成功率
                    S(1) / T(1)
                重抓的成功率
                    S(12) / T(12)
            2. 清洗
                总成功率 S(2) / T(2)
            3. 入库
                总成功率 S(3) / T(3)
            :return:
        """
        # 总成功率
        total_log_count = query.count()
        total_success_log_count = query.filter(ExtLogInfo.result == 1).count()
        total_success_rate = total_success_log_count / self.is_zero(total_log_count)

        result['total_success_rate'] = total_success_rate

        # 抓数总成功率
        extract_data_total_log_count = query.filter(or_(ExtLogInfo.task_type == 1,
                                                        ExtLogInfo.task_type == 12)).count()
        extract_data_total_success_log_count = query.filter(ExtLogInfo.result == 1).filter(
            or_(ExtLogInfo.task_type == 1,
                ExtLogInfo.task_type == 12)
        ).count()

        extract_data_total_success_rate = extract_data_total_success_log_count / self.is_zero(extract_data_total_log_count)

        result['extract_data_total_success_rate'] = extract_data_total_success_rate

        # 首次抓数的成功率
        first_extract_data_total_log_count = query.filter(ExtLogInfo.task_type == 1).count()
        first_extract_data_success_log_count = query.filter(ExtLogInfo.task_type == 1).filter(
            ExtLogInfo.result == 1
        ).count()

        first_extract_data_success_rate = first_extract_data_success_log_count / self.is_zero(first_extract_data_total_log_count)

        result['first_extract_data_success_rate'] = first_extract_data_success_rate

        # 重抓的成功率
        retry_extract_data_total_log_count = query.filter(ExtLogInfo.task_type == 12).count()
        retry_extract_data_success_log_count = query.filter(ExtLogInfo.task_type == 12).filter(
            ExtLogInfo.result == 1
        ).count()
        retry_extract_data_success_rate = retry_extract_data_success_log_count / self.is_zero(retry_extract_data_total_log_count)
        result['retry_extract_data_success_rate'] = retry_extract_data_success_rate

        # 清洗的成功率

        clean_total_log_count = query.filter(ExtLogInfo.task_type == 2).count()
        clean_success_log_count = query.filter(ExtLogInfo.task_type == 2).filter(
            ExtLogInfo.result == 1
        ).count()
        clean_success_rate = clean_success_log_count / self.is_zero(clean_total_log_count)
        result['clean_success_rate'] = clean_success_rate

        # 入库的成功率
        load_total_log_count = query.filter(ExtLogInfo.task_type == 3).count()
        load_success_log_count = query.filter(ExtLogInfo.task_type == 3).filter(
            ExtLogInfo.result == 1
        ).count()
        load_success_rate = load_success_log_count / self.is_zero(load_total_log_count)
        result['load_success_rate'] = load_success_rate

        return result

    def is_zero(self, number):
        return number if number else 1
