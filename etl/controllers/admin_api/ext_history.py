from .. import jsonify_with_error, jsonify_with_data, APIError
from . import etl_admin_api
from etl.service.ext_history import ExtHistoryServices, ExtHistoryParameterMiss, ExtHistoryDateError
service = ExtHistoryServices()


@etl_admin_api.route("/ext/history/table", methods=["GET"])
def get_table():
    """
    返回抓数任务供选择的目标表
    :return:
    """
    try:
        tables = service.get_table()
    except Exception as e:
        return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    return jsonify_with_data(APIError.OK, data={"tables": tables})


@etl_admin_api.route("/ext/history/task/start", methods=["POST"])
def start_task():
    """
    开始任务，任务有三种类型：抓数，清洗和入库，全做
    :return:
    """
    try:
        service.start_task()
    except (ExtHistoryParameterMiss, ExtHistoryDateError) as e:
        return jsonify_with_error(APIError.VALIDATE_ERROR, reason=str(e))
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext/history/task/stop", methods=["GET"])
def stop_task():
    """
    停止任务
    :return:
    """
    service.stop_stak()
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext/history/tasks", methods=["GET"])
def get_tasks():
    """
    查看所有的任务列表
    :return:
    """
    try:
        data = service.get_tasks()
    except Exception as e:
        return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    return jsonify_with_data(APIError.OK, data=data)


@etl_admin_api.route("/ext/history/task/running", methods=["GET"])
def get_task_running():
    """
    查询正在进行中的所有任务
    :return:
    """
    # try:
    data = service.get_task_running()
    # except Exception as e:
    #     return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    return jsonify_with_data(APIError.OK, data=data)


@etl_admin_api.route("/ext/history/task/log", methods=["GET"])
def get_task_log():
    """查询单个任务的每一天的日志记录"""

    data = service.get_task_log()
    return jsonify_with_data(APIError.OK, data=data)