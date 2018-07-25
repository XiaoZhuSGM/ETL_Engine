from . import etl_admin_api, jsonify_with_data
from flask import request
from . import APIError
from etl.service.ext_table import ExtTableService
from etl.validators.validator import validate_arg, JsonExtTableInput


ext_table_service = ExtTableService()


@etl_admin_api.route('/tables/download', methods=['POST'])
# @validate_arg(JsonExtTableInput)
def download_tables():
    """
      "db_name": [{"database": "sss","schema": ["schema1", "schema2"]},
      {"database": "sss2","schema": ["schema1", "schema2"]},
      ...
      ]

    :return:
    """

    data = request.get_json()
    cmid = data.get('cmid')
    data = ext_table_service.get_datasource(cmid)
    db_name = data.get('db_name', [])
    for db_dict in db_name:
        database = db_dict.get('database')
        data['database'] = database

        schema_list = db_dict.get('schema')

        if not schema_list:
            ext_table_service.download_table(**data)
            continue
        for scheam in schema_list:
            data['schema'] = scheam
            ext_table_service.download_table(**data)
    return jsonify_with_data(APIError.OK)




