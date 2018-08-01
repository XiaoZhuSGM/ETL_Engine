from . import etl_admin_api, jsonify_with_data, jsonify_with_error
from .. import APIError
from etl.service.ext_table import ExtTableService
from multiprocessing import Process, Lock

lock = Lock()


@etl_admin_api.route('/tables/download/<int:cmid>', methods=['GET'])
def download_tables(cmid):

    """
      "db_name": [{"database": "sss","schema": ["schema1", "schema2"]},
      {"database": "sss2","schema": ["schema1", "schema2"]},
      ...
      ]

    :return:
    """
    ext_table_service = ExtTableService()

    # 如果任务已经再，就不重复执行
    status = ext_table_service.get_status(str(cmid))
    if status == 'running':
        return jsonify_with_data(APIError.BAD_REQUEST, reason="任务已经在执行，请勿重复执行")

    # 测试数据库是否能够正常连接，只要有一个无法连接，就返回错误信息
    try:
        data = ext_table_service.get_datasource_by_cmid(cmid)
    except Exception as e:
        return jsonify_with_error(APIError.NOTFOUND, repr(e))

    db_name = data.get('db_name', [])
    for db_dict in db_name:
        database = db_dict.get('database')
        data['database'] = database
        error = ext_table_service.connect_test(**data)
        if error:
            return jsonify_with_error(APIError.BAD_REQUEST, reason=error)

    task = Process(target=ext_table_service.download_tables, args=(lock,), kwargs=data)
    task.start()

    return jsonify_with_data(APIError.OK)


@etl_admin_api.route('/tables/download/status/<int:cmid>', methods=['GET'])
def get_download_tables_status(cmid):
    ext_table_service = ExtTableService()
    cmid = str(cmid)
    status = ext_table_service.get_status(cmid)

    if status:
        return jsonify_with_data(APIError.OK, data={'status': status})
    else:
        return jsonify_with_error(APIError.NOTFOUND, reason='no task')
