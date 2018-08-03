from flask import request
from . import etl_admin_api
from etl.validators import validate_arg
from etl.validators.ext_datasource_con import (
    GetExtDatasourceCon,
    CreateExtDatasourceCon,
    ModifyExtDatasourceCon,
)
from etl.controllers import APIError, jsonify_with_data, jsonify_with_error
from etl.service.ext_datasource_con import (
    ExtDatasourceConService,
    ExtDatasourceConNotExist,
)

service = ExtDatasourceConService()


@etl_admin_api.route("/ext_datasource_con", methods=["GET"])
@validate_arg(GetExtDatasourceCon)
def get_ext_datasource_con():
    try:
        ext_datasource_con = service.get_ext_datasource_con(request.args["source_id"])
    except ExtDatasourceConNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data=ext_datasource_con)


@etl_admin_api.route("/ext_datasource_con", methods=["POST"])
@validate_arg(CreateExtDatasourceCon)
def create_ext_datasource_con():
    data = request.json
    service.create_ext_datasource_con(data)
    return jsonify_with_data(APIError.OK, data={})


@etl_admin_api.route("/ext_datasource_con/<int:id>", methods=["PATCH"])
@validate_arg(ModifyExtDatasourceCon)
def modify_ext_datasource_con(id):
    data = request.json
    try:
        service.modify_ext_datasource_con(id, data)
    except ExtDatasourceConNotExist as e:
        return jsonify_with_error(APIError.NOTFOUND, str(e))
    return jsonify_with_data(APIError.OK, data={})
