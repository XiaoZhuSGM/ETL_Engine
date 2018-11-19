from etl.service.ext_table import (
    ExtTableService, ExtTaskExists, ExtDatasourceParaMiss, ExtDatasourceConnError, ExtDataSourceNotFound
)
from . import etl_admin_api, jsonify_with_data, jsonify_with_error
from .. import APIError
from flask import current_app
from threading import Thread
service = ExtTableService()


@etl_admin_api.route('/tables/download/<source_id>', methods=['GET'])
def download_tables(source_id):
    """
      "db_name": {"database": "sss","schema": ["schema1", "schema2"]},
        只考虑单数据库，多schema的情况，不考虑多数据库
    """
    try:
        service.generate_download_table(source_id)
    except ExtTaskExists as e:
        return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    except (ExtDatasourceParaMiss, ExtDatasourceConnError, ExtDataSourceNotFound) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, reason=str(e))
    return jsonify_with_data(APIError.OK)


@etl_admin_api.route('/tables/download/status/<source_id>', methods=['GET'])
def get_download_tables_status(source_id):
    status = service.get_status(source_id)

    if status:
        return jsonify_with_data(APIError.OK, data={'status': status})
    else:
        return jsonify_with_data(APIError.OK, data={'status': 'no task'})


@etl_admin_api.route('/tables/download/special', methods=['POST'])
def download_special_table():
    """
    指定获取某些表的表结构
    :return:
    """
    try:
        service.download_special_tables()
    except ExtTaskExists as e:
        return jsonify_with_error(APIError.SERVER_ERROR, reason=str(e))
    except (ExtDatasourceParaMiss, ExtDatasourceConnError, ExtDataSourceNotFound) as e:
        return jsonify_with_error(APIError.BAD_REQUEST, reason=str(e))
    return jsonify_with_data(APIError.OK)


@etl_admin_api.route("/tables/download/date_table", methods=["GET"])
def download_date_table():
    """
    抓取和时间相关的表，在每个月1，2，3号运行
    """
    ext_table_service = ExtTableService()

    task = Thread(target=ext_table_service.download_about_date_table,
                  args=(current_app._get_current_object(),))

    task.start()
    return jsonify_with_data(APIError.OK, data={})
