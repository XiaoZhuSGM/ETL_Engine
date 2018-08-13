from flask import request

from etl.service.ext_log import ExtLogSqlService
from . import etl_admin_api
from .. import jsonify_with_error, jsonify_with_data, APIError
from ...validators.validator import validate_arg, PageInput

service = ExtLogSqlService()


@etl_admin_api.route("/ext/log", methods=["POST"])
def add_log():
    """
    task_type --> 0: 抓数,  1--> 入库
    {"source_id":"32YYYYYYYYYYYYY","cmid":3201,"task_type":1,"table_name":"goods","record_num":120,
    "start_time":1111111111111,"end_time":11111111222,"cost_time":111,"result":1}
    :return:
    """
    kwargs = request.json["logs"]
    log = service.add_log(**kwargs)
    if log is not None:
        return jsonify_with_data(APIError.OK)


@etl_admin_api.route("/ext/log", methods=["GET"])
@validate_arg(PageInput)
def get_log():
    """
    根据条件搜索日志
    source_id, table_name, task_type, begin_time, end_time, result
    可以根据此字段查询失败的日志(result)
    分页字段 ，page, per_page
    :return:
    """
    result = service.get_log(request.args)
    return jsonify_with_data(APIError.OK, data=result)
