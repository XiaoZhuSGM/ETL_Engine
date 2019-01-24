from ..models.etl_table import ExtHistoryTask, ExtHistoryLog, ExtCleanInfo
from flask import request
from datetime import timedelta
from etl.celery_tasks.ext_history_tasks import start_tasks
from datetime import datetime
from etl.constant import PER_PAGE
from etl.etl import celery
from etl.models import session_scope


class ExtCleanInfoParameterError(Exception):
    def __str__(self):
        return "parameter error"


class ExtHistoryTaskNotFound(Exception):
    def __str__(self):
        return "task is not exist"


class ExtHistoryParameterMiss(Exception):
    def __str__(self):
        return "数据库id,任务类型和表名称是必选项，请重新选择"


class ExtHistoryDateError(Exception):
    def __str__(self):
        return "数据开始日期或数据结束日期选择错误，请重新选择"


class ExtHistoryServices:
    def get_table(self):
        """获取目标表"""
        source_id = request.args.get("source_id")
        tables = [
            model.target_table for model in ExtCleanInfo.query.filter_by(source_id=source_id, deleted=False).all()
        ]
        tables.sort()
        return tables

    def start_task(self):
        """开始任务"""
        data = request.get_json()
        source_id = data.get("source_id")
        task_type = data.get("task_type")
        start_date = data.get("start_date") if data.get("start_date") \
            else (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        end_date = data.get("end_date") if data.get("end_date") \
            else (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        target_tables = data.get("target_tables")

        if not all([source_id, task_type, target_tables]):
            raise ExtHistoryParameterMiss()

        if not(start_date <= end_date) or not(end_date <= datetime.now().strftime('%Y-%m-%d')):
            raise ExtHistoryDateError()

        task = start_tasks.delay(dict(source_id=source_id, task_type=task_type, start_date=start_date,
                               end_date=end_date, target_tables=target_tables))
        return {'id': task.id}

    def get_tasks(self):
        """获取任务记录"""
        source_id = request.args.get("source_id")
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", PER_PAGE))

        query = ExtHistoryTask.query
        if source_id:
            query = query.filter_by(source_id=source_id)

        if per_page != -1:
            pagination = query.order_by(ExtHistoryTask.created_at.desc()).paginate(page=page, per_page=per_page,
                                                                            error_out=False)
            items = pagination.items
            total = pagination.total
        else:
            items = query.all()
            total = len(items)
        return dict(items=[task.to_dict() for task in items], total=total)

    @session_scope
    def stop_stak(self):
        """停止任务"""
        task_id = request.args.get("task_id")
        celery.control.revoke(task_id, terminate=True)
        self.set_task_status(task_id, 2)

    @session_scope
    def get_task_running(self):
        """获取正在进行中的任务"""
        history_tasks = ExtHistoryTask.query.filter_by(status=3).all()

        data = []
        for model in history_tasks:
            task = start_tasks.AsyncResult(model.task_id)
            print(task.state)
            if task.state == "RUNNING" or task.state == "STARTED":
                percentage = round(float((task.info.get("total") - task.info.get("pending"))*100
                                         /task.info.get("total")), 2) if task.info.get("total") != 0 else 1

            elif task.state == "PENDING":
                percentage = 0
            else:
                self.set_task_status(model.task_id, 1)
                continue
            data.append({
                "state": task.state,
                "percentage": percentage,
                "source_id": model.source_id,
                "task_id": model.task_id,
                "ext_start": model.ext_start,
                "ext_end": model.ext_end,
                "task_type": model.task_type,
                "target_table": model.target_table,
            })

        return data

    def set_task_status(self, task_id, status):
        """设置任务状态"""
        ext_history_task = ExtHistoryTask.query.filter_by(task_id=task_id).first()
        if not ext_history_task.task_end:
            ext_history_task.task_end = datetime.now()
        ext_history_task.status = status
        ext_history_task.save()

    def get_task_log(self):
        """查询单个任务的每一天的日志记录"""
        source_id = request.args.get("source_id")
        task_id = request.args.get("task_id")
        result = request.args.get("result")
        ext_date = request.args.get("ext_date")
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", PER_PAGE))

        query = ExtHistoryLog.query
        if source_id:
            query = query.filter_by(source_id=source_id)

        if task_id:
            query = query.filter_by(task_id=task_id)

        if result:
            result = int(result)
            query = query.filter_by(result=result)
        if ext_date:
            query = query.filter_by(ext_date=ext_date)

        if per_page != -1:
            pagination = query.order_by(ExtHistoryLog.created_at.desc()).paginate(
                page=page, per_page=per_page, error_out=False)
            items = pagination.items
            total = pagination.total
        else:
            items = query.all()
            total = len(items)

        return dict(items=[task.to_dict() for task in items], total=total)
