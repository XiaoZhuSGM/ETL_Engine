from flask import jsonify

from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.models import session_scope
from etl.service.ext_table_check import ExtCheckTable

from . import etl_admin_api
from etl.models.etl_table import ExtTestQuery, ExtCheckNum

import arrow


@etl_admin_api.route("/ext/check/<source_id>")
def get_ext_check(source_id):
    ext_check_table = ExtCheckTable()
    data = ext_check_table.get_datasource_by_source_id(source_id)
    if data is None:
        return jsonify_with_error(APIError.NOTFOUND, "Datasource not found")

    # 测试数据库是否能够正常连接，无法连接就返回错误信息
    db_name = data.get('db_name')
    if db_name is None:
        return jsonify_with_error(APIError.BAD_REQUEST, "db_name is missing")
    database = db_name.get('database')
    if database is None:
        return jsonify_with_error(APIError.BAD_REQUEST, "database is missing")

    data['database'] = database
    error = ext_check_table.connect_test(**data)
    if error:
        return jsonify_with_error(APIError.BAD_REQUEST, error)

    # 查看本地数据库是否有数
    date = arrow.utcnow().shift(days=-1).format('YYYY-MM-DD')
    info = {
        "source_id": source_id,
        "date": date,
    }
    my_data = ExtCheckNum.query.filter_by(source_id=source_id, date=date).first()
    if my_data:
        num = my_data.num
        if num > 500:
            return jsonify_with_data(APIError.OK, data={'result': num})
        else:
            num = ext_check_table.get_target_num(source_id, date)
            ext_check_table.modify_check_num(source_id, num, date)
    else:
        num = ext_check_table.get_target_num(source_id, date)
        info['num'] = num
        ext_check_table.create_check_num(info)

    return jsonify_with_data(APIError.OK, data={'num': num})
