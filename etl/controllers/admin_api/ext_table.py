from threading import Thread

from flask import current_app

from etl.service.ext_table import ExtTableService
from . import etl_admin_api, jsonify_with_data, jsonify_with_error
from .. import APIError


@etl_admin_api.route('/tables/download/<source_id>', methods=['GET'])
def download_tables(source_id):
    """
      "db_name": [{"database": "sss","schema": ["schema1", "schema2"]},
      {"database": "sss2","schema": ["schema1", "schema2"]},
      ...
      ]

    :return:
    """
    ext_table_service = ExtTableService()

    # 如果任务已经再，就不重复执行
    status = ext_table_service.get_status(source_id)
    if status == 'running':
        return jsonify_with_data(APIError.PROCESSING, reason="task is running")

    # 测试数据库是否能够正常连接，只要有一个无法连接，就返回错误信息
    data = ext_table_service.get_datasource_by_source_id(source_id)
    if data is None:
        return jsonify_with_error(APIError.NOTFOUND, "Datasource not found")

    db_name = data.get('db_name', [])
    for db_dict in db_name:
        database = db_dict.get('database')
        data['database'] = database
        error = ext_table_service.connect_test(**data)
        if error:
            return jsonify_with_error(APIError.BAD_REQUEST, reason=error)

    task = Thread(target=ext_table_service.download_tables,
                  args=(current_app._get_current_object(),), kwargs=data)

    task.start()

    return jsonify_with_data(APIError.OK)


@etl_admin_api.route('/tables/download/status/<source_id>', methods=['GET'])
def get_download_tables_status(source_id):
    ext_table_service = ExtTableService()
    status = ext_table_service.get_status(source_id)

    if status:
        return jsonify_with_data(APIError.OK, data={'status': status})
    else:
        return jsonify_with_data(APIError.OK, data={'status': 'no task'})
