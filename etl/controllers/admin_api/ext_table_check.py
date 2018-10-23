from flask import request

from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.service.ext_table_check import ExtCheckTable

from . import etl_admin_api
from etl.models.etl_table import ExtCheckNum

import arrow

ext_check_table = ExtCheckTable()


@etl_admin_api.route("/ext/check/<source_id>")
def get_ext_check(source_id):
    date = request.args.get("date")
    if date is None:
        date = arrow.now().shift(days=-1).format('YYYY-MM-DD')

    source_id = source_id.upper()

    # 查看本地数据库是否有数
    my_data = ExtCheckNum.query.filter_by(source_id=source_id, date=date).first()
    if my_data and my_data.num > 500:
        return jsonify_with_data(APIError.OK, data={'num': my_data.num})

    # 测试数据库是否能够正常连接，无法连接就返回错误信息
    data_source = ext_check_table.get_datasource_by_source_id(source_id)
    if data_source is None:
        return jsonify_with_error(APIError.NOTFOUND, "source_id 有误, 请检查source_id是否正确")
    db_name = data_source.get('db_name')
    if db_name is None:
        return jsonify_with_error(APIError.BAD_REQUEST, "db_name is missing")
    database = db_name.get('database')
    if database is None:
        return jsonify_with_error(APIError.BAD_REQUEST, "database is missing")
    data_source['database'] = database
    error = ext_check_table.connect_test(**data_source)
    if error:
        return jsonify_with_error(APIError.BAD_REQUEST, "对方数据库连接失败！")
    num = ext_check_table.get_target_num(source_id, date)
    if num is None:
        return jsonify_with_error(APIError.NOTFOUND, "sql not found")

    ext_check_table.create_check_num(source_id, num, date)

    return jsonify_with_data(APIError.OK, data={'num': num})
