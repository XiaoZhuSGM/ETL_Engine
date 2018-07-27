
from . import etl_admin_api, jsonify_with_data, jsonify_with_error
from . import APIError
from etl.service.ext_table import ExtTableService


ext_table_service = ExtTableService()


@etl_admin_api.route('/tables/download/<int:cmid>', methods=['GET'])
def download_tables(cmid):

    """
      "db_name": [{"database": "sss","schema": ["schema1", "schema2"]},
      {"database": "sss2","schema": ["schema1", "schema2"]},
      ...
      ]

    :return:
    """
    try:
        data = ext_table_service.get_datasource(cmid)
    except Exception as e:
        return jsonify_with_error(APIError.NOTFOUBD, repr(e))

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




