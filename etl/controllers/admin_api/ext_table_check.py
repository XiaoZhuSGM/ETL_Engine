from flask import request

from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.service.ext_table_check import ExtCheckTable

from . import etl_admin_api
from etl.models.etl_table import ExtCheckNum

import arrow

ext_check_table = ExtCheckTable()


@etl_admin_api.route("/ext/check/<source_id>/<target_table>")
def get_ext_check(source_id, target_table):
    date = request.args.get("date")
    if date is None:
        date = arrow.now().shift(days=-1).format('YYYY-MM-DD')

    source_id = source_id.upper()
    target_table = target_table.lower()

    # 查看本地数据库是否有数
    my_data = ExtCheckNum.query.filter_by(source_id=source_id, date=date, target_table=target_table).first()
    if my_data and my_data.num > 500:
        return jsonify_with_data(APIError.OK, data={'num': my_data.num})

    if (source_id in ['59YYYYYYYYYYYYY', '70YYYYYYYYYYYYY', '73YYYYYYYYYYYYY']) and target_table == 'goodsflow':
        num = ext_check_table.ext_serial(source_id, date)
        return jsonify_with_data(APIError.OK, data={'num': num})

    # 测试数据库是否能够正常连接，无法连接就返回错误信息
    data_source = ext_check_table.get_datasource_by_source_id(source_id)
    if data_source is None:
        return jsonify_with_error(APIError.NOTFOUND, "source_id 有误, 请检查source_id是否正确")
    db_name = data_source.get('db_name')
    database = db_name.get('database')
    if not all([db_name, database]):
        return jsonify_with_error(APIError.BAD_REQUEST, "数据库信息不全，请检查")

    data_source['database'] = database

    error = ext_check_table.connect_test(**data_source)
    if error:
        return jsonify_with_error(APIError.BAD_REQUEST, "对方数据库连接失败！")
    num = ext_check_table.get_target_num(source_id, target_table, date)
    if not num:
        return jsonify_with_error(APIError.NOTFOUND, "sql not found")

    num = list(num[0].values())[0]
    ext_check_table.create_check_num(source_id, target_table, num, date)

    return jsonify_with_data(APIError.OK, data={'num': num})


@etl_admin_api.route("/ext/sql", methods=['POST'])
def create_sql():
    data = request.json
    sql = data.get('sql')
    source_id = data.get('source_id')
    if not all([source_id, sql]):
        return jsonify_with_error(APIError.VALIDATE_ERROR, '参数不全')

    data_source = ext_check_table.get_datasource_by_source_id(source_id.upper())
    if data_source is None:
        return jsonify_with_error(APIError.NOTFOUND, "source_id 有误, 请检查source_id是否正确")
    db_name = data_source.get('db_name')
    database = db_name.get('database')
    if not all([db_name, database]):
        return jsonify_with_error(APIError.BAD_REQUEST, "数据库信息不全，请检查")

    data_source['database'] = database
    error = ext_check_table.connect_test(**data_source)
    if error:
        return jsonify_with_error(APIError.BAD_REQUEST, "对方数据库连接失败！")

    result = ext_check_table.execute_sql(sql)
    if result == "error":
        return jsonify_with_error(APIError.VALIDATE_ERROR, 'sql错误！')
    for i in result:
        table_head = [k for k in i.keys()]
        return jsonify_with_data(APIError.OK, data={'table_head': table_head, 'result': result})


@etl_admin_api.route("/save/sql", methods=['POST'])
def ext_save_sql():
    data = request.json
    sql = data.get('sql')
    target_table = data.get('target_table')
    source_id = data.get('source_id')
    if not all([sql, source_id]):
        return jsonify_with_error(APIError.VALIDATE_ERROR, '参数有误')

    ext_check_table.create_test_query(source_id, sql, target_table)
    return jsonify_with_data(APIError.OK, data={'result': '{}保存成功'.format(source_id)})
