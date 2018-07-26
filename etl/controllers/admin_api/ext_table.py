from . import etl_admin_api, jsonify_with_data
from flask import request
from . import APIError, jsonify_with_error
from etl.service.ext_table import ExtTableService
from etl.validators.validator import validate_arg, JsonExtTableInput

ext_table_service = ExtTableService()


@etl_admin_api.route('/tables/download', methods=['POST'])
@validate_arg(JsonExtTableInput)
def download_tables():
    database = request.get_json()
    cmid = database.get('cmid')
    res = ext_table_service.connect_test(**database)
    if res is not None:
        return jsonify_with_error(APIError.DBCONNECTFALSE, reason=res)
    tables = ext_table_service.get_tables()
    for table in tables:
        ext_pri_key = ext_table_service.get_ext_pri_key(table)
        ext_column = ext_table_service.get_ext_column(table)
        record_num = ext_table_service.get_record_num(table)
        table_info = ext_table_service.get_table_from_pgsql(cmid=cmid, table_name=table)
        try:
            table_info = table_info[0]
        except Exception as e:
            ext_table_service.create_ext_table(
                cmid=cmid, table_name=table, ext_pri_key=ext_pri_key, ext_column=ext_column, record_num=record_num)
            continue

        if table_info.ext_pri_key == ext_pri_key and table_info.ext_column == ext_column and table_info.record_num == record_num:
            continue
        ext_table_service.update_ext_table(table_info, ext_pri_key=ext_pri_key, ext_column=ext_column, record_num=record_num)

    return jsonify_with_data(APIError.OK)




