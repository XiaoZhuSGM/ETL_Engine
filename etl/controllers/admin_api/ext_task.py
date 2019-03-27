from etl.tasks.tasks import task_warehouse, task_extract_data, task_extract_inventory, task_load_inv
from etl.tasks.rollback_taskset import task_rollback
from . import etl_admin_api
from .. import jsonify_with_data, APIError
from flask import request
import json
import boto3
from etl.service.ext_datasource_con import ExtDatasourceConService

service = ExtDatasourceConService()

S3_BUCKET = "ext-etl-data"
TARGET_TABLE_KEY = "target_table/target_table.json"
S3_CLIENT = boto3.resource("s3")


@etl_admin_api.route("/ext/tasks/extract_data", methods=["POST"])
def trigger_task_extract_data():
    message = request.json
    source_id = message["source_id"]
    query_date = message["query_date"]
    task_type = message["task_type"]
    filename = message["filename"]
    db_url = message["db_url"]

    event = dict(
        source_id=source_id,
        query_date=query_date,
        task_type=task_type,
        filename=filename,
        db_url=db_url,
    )

    result = task_extract_data.apply_async(kwargs=event)
    return jsonify_with_data(APIError.OK, data={"task_id": result.id})


@etl_admin_api.route("/ext/tasks/extract_data/status", methods=["GET"])
def get_task_extract_data_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = False
    async_result = task_extract_data.AsyncResult(task_id=task_id)

    if async_result.successful():
        result = async_result.result
        status = "success"
    elif async_result.failed():
        status = "failed"
        reason = str(async_result.info)
    else:
        status = "running"
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )


@etl_admin_api.route("/ext/tasks/warehouse", methods=["POST"])
def trigger_task_warehouse():
    message = request.json
    redshift_url = message["redshift_url"]
    data_key = message["data_key"]
    target_table = message["target_table"]
    warehouse_type = message["warehouse_type"]
    source_id = message["source_id"]
    cmid = message["cmid"]
    target_tables = json.loads(
        S3_CLIENT.Object(S3_BUCKET, TARGET_TABLE_KEY)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    table_key = (
        target_table
        if not target_table.endswith(source_id)
        else target_table.split(f"_{source_id}")[0]
    )
    sync_column = target_tables[table_key]["sync_column"]
    date_column = target_tables[table_key]["date_column"]

    event = dict(
        db_url=redshift_url,
        target_table=target_table,
        data_key=data_key,
        sync_column=sync_column,
        date_column=date_column,
        cmid=cmid,
        source_id=source_id,
        warehouse_type=warehouse_type,
    )

    result = task_warehouse.apply_async(kwargs=event)

    return jsonify_with_data(APIError.OK, data={"task_id": result.id})


@etl_admin_api.route("/ext/tasks/warehouse/status", methods=["GET"])
def get_task_warehouse_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = False
    async_result = task_warehouse.AsyncResult(task_id=task_id)
    if async_result.successful():
        status = "success"
        result = async_result.result
    elif async_result.failed():
        status = "failed"
        reason = str(async_result.info)
    else:
        status = "running"
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )


@etl_admin_api.route("/ext/tasks/rollback", methods=["POST"])
def trigger_task_rollback():
    message = request.json
    event = dict(
        source_id=message["source_id"],
        date=message["date"],
        erp_name=message["erp_name"],
        target_list=message["target_list"],
    )

    result = task_rollback.apply_async(kwargs=event)
    return jsonify_with_data(APIError.OK, data={"task_id": result.id})


@etl_admin_api.route("/ext/tasks/rollback/status", methods=["GET"])
def get_task_rollback_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = False
    async_result = task_rollback.AsyncResult(task_id=task_id)
    if async_result.successful():
        status = "success"
        result = async_result.result
    elif async_result.failed():
        status = "failed"
        reason = str(async_result.info)
    else:
        status = "running"
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )


@etl_admin_api.route("/ext/tasks/extract_inventory", methods=["POST"])
def trigger_task_extract_inventory():
    message = request.json
    source_id = message["source_id"]
    query_date = message["query_date"]
    task_type = message["task_type"]
    filename = message["filename"]
    db_url = message["db_url"]
    event = dict(
        source_id=source_id,
        query_date=query_date,
        task_type=task_type,
        filename=filename,
        db_url=db_url,
    )
    result = task_extract_inventory.apply_async(kwargs=event)
    return jsonify_with_data(APIError.OK, data={"task_id": result.id})


@etl_admin_api.route("/ext/tasks/extract_inventory/status", methods=["GET"])
def get_task_extract_inventory_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = False
    async_result = task_extract_inventory.AsyncResult(task_id=task_id)

    if async_result.successful():
        result = async_result.result
        status = "success"
    elif async_result.failed():
        status = "failed"
        reason = str(async_result.info)
    else:
        status = "running"
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )


@etl_admin_api.route("/ext/tasks/load_inv", methods=["POST"])
def trigger_task_load_inv():
    message = request.json
    redshift_url = message["redshift_url"]
    data_key = message["data_key"]
    target_table = message["target_table"]
    warehouse_type = message["warehouse_type"]
    data_date = message["data_date"]
    source_id = message["source_id"]
    cmid = message["cmid"]
    target_tables = json.loads(
        S3_CLIENT.Object(S3_BUCKET, TARGET_TABLE_KEY)
            .get()["Body"]
            .read()
            .decode("utf-8")
    )
    table_key = (
        target_table
        if not target_table.endswith(source_id)
        else target_table.split(f"_{source_id}")[0]
    )
    sync_column = target_tables[table_key]["sync_column"]
    date_column = target_tables[table_key]["date_column"]

    event = dict(
        db_url=redshift_url,
        target_table=target_table,
        data_key=data_key,
        sync_column=sync_column,
        date_column=date_column,
        cmid=cmid,
        source_id=source_id,
        warehouse_type=warehouse_type,
        data_date=data_date,
    )

    result = task_load_inv.apply_async(kwargs=event)

    return jsonify_with_data(APIError.OK, data={"task_id": result.id})


@etl_admin_api.route("/ext/tasks/load_inv/status", methods=["GET"])
def get_task_load_inv_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = False
    async_result = task_load_inv.AsyncResult(task_id=task_id)
    if async_result.successful():
        status = "success"
        result = async_result.result
    elif async_result.failed():
        status = "failed"
        reason = str(async_result.info)
    else:
        status = "running"
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )
